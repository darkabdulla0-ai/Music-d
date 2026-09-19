import asyncio
import logging
import os
from collections import deque
from dataclasses import dataclass
from typing import Optional

import discord
import yt_dlp
from discord.ext import commands
from dotenv import load_dotenv


# يقرأ DISCORD_TOKEN من ملف .env محلياً أو من Secrets في Replit.
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
logger = logging.getLogger("discord_music_bot")


intents = discord.Intents.default()
intents.message_content = True
intents.voice_states = True

bot = commands.Bot(
    command_prefix="!",
    intents=intents,
    help_command=None,
)


YTDL_OPTIONS = {
    "format": "bestaudio/best",
    "noplaylist": True,
    "quiet": True,
    "no_warnings": True,
    "source_address": "0.0.0.0",
}

FFMPEG_OPTIONS = {
    "before_options": (
        "-reconnect 1 "
        "-reconnect_streamed 1 "
        "-reconnect_delay_max 5"
    ),
    "options": "-vn",
}


@dataclass
class Track:
    """بيانات أغنية واحدة داخل الطابور."""

    title: str
    stream_url: str
    webpage_url: str
    requested_by: str


class GuildMusicState:
    """حالة الموسيقى الخاصة بكل سيرفر بشكل مستقل."""

    def __init__(self) -> None:
        self.queue: deque[Track] = deque()
        self.current: Optional[Track] = None
        self.player_task: Optional[asyncio.Task] = None
        self.text_channel: Optional[discord.TextChannel] = None
        self.stop_requested = False


music_states: dict[int, GuildMusicState] = {}


def get_music_state(guild_id: int) -> GuildMusicState:
    """يرجع حالة السيرفر أو ينشئها إذا كانت أول مرة."""

    if guild_id not in music_states:
        music_states[guild_id] = GuildMusicState()
    return music_states[guild_id]


async def extract_track(search: str, requester: str) -> Track:
    """
    يستخرج أول نتيجة من SoundCloud أو يتعامل مع الرابط المباشر.

    yt-dlp يستخدم scsearch1: للبحث عن أول نتيجة في SoundCloud.
    """

    query = search.strip()
    if not query:
        raise ValueError("اكتب اسم الأغنية أو رابطها.")

    # إذا لم يكن الإدخال رابطاً، نضيف بادئة بحث SoundCloud الصحيحة.
    target = query
    if yt_dlp.utils.url_or_none(query) is None:
        target = f"scsearch1:{query}"

    def extract() -> dict:
        with yt_dlp.YoutubeDL(YTDL_OPTIONS) as ytdl:
            return ytdl.extract_info(target, download=False)

    data = await asyncio.to_thread(extract)

    if not data:
        raise RuntimeError("لم يتم العثور على نتيجة.")

    if "entries" in data:
        entries = [entry for entry in data["entries"] if entry]
        if not entries:
            raise RuntimeError("لم يتم العثور على أغنية بهذا الاسم.")
        data = entries[0]

    stream_url = data.get("url")
    if not stream_url:
        raise RuntimeError("تعذر الحصول على رابط الصوت من SoundCloud.")

    return Track(
        title=data.get("title") or "أغنية بدون عنوان",
        stream_url=stream_url,
        webpage_url=data.get("webpage_url") or query,
        requested_by=requester,
    )


async def send_message(
    channel: Optional[discord.abc.Messageable],
    content: str,
) -> None:
    """يرسل رسالة مع تجاهل أخطاء القناة المحذوفة أو غير المتاحة."""

    if channel is None:
        return

    try:
        await channel.send(content)
    except (discord.Forbidden, discord.NotFound, discord.HTTPException):
        logger.warning("تعذر إرسال رسالة إلى القناة.")


async def player_loop(
    guild: discord.Guild,
    state: GuildMusicState,
) -> None:
    """يشغل الأغاني بالتتابع حتى ينتهي الطابور أو يطلب المستخدم الإيقاف."""

    try:
        while state.queue and not state.stop_requested:
            track = state.queue.popleft()
            state.current = track
            voice_client = guild.voice_client

            if voice_client is None or not voice_client.is_connected():
                await send_message(
                    state.text_channel,
                    "❌ البوت غير متصل بالروم الصوتي، تم إيقاف الطابور.",
                )
                break

            finished = asyncio.Event()
            playback_error: Optional[Exception] = None

            def after_playback(error: Optional[Exception]) -> None:
                nonlocal playback_error
                playback_error = error
                bot.loop.call_soon_threadsafe(finished.set)

            try:
                audio_source = discord.PCMVolumeTransformer(
                    discord.FFmpegPCMAudio(
                        track.stream_url,
                        **FFMPEG_OPTIONS,
                    ),
                    volume=0.5,
                )
                voice_client.play(audio_source, after=after_playback)
            except Exception as error:
                logger.exception("فشل تشغيل الأغنية.")
                await send_message(
                    state.text_channel,
                    f"⚠️ تعذر تشغيل **{track.title}**: `{error}`",
                )
                state.current = None
                continue

            await send_message(
                state.text_channel,
                f"🎶 الآن يعمل: **{track.title}**\n"
                f"👤 طلبها: {track.requested_by}",
            )

            await finished.wait()

            # أخطاء الإيقاف اليدوي لا تحتاج رسالة إضافية.
            if playback_error and not state.stop_requested:
                logger.warning(
                    "حدث خطأ أثناء تشغيل %s: %s",
                    track.title,
                    playback_error,
                )
                await send_message(
                    state.text_channel,
                    f"⚠️ انتهى تشغيل **{track.title}** بسبب خطأ، "
                    "وسأنتقل للأغنية التالية.",
                )

            state.current = None
    except asyncio.CancelledError:
        # يستخدم stop هذا الإلغاء حتى لا يبدأ الطابور من جديد بعد الخروج.
        raise
    finally:
        state.current = None
        current_task = asyncio.current_task()
        if state.player_task is current_task:
            state.player_task = None


async def ensure_voice_connection(
    ctx: commands.Context,
) -> Optional[discord.VoiceClient]:
    """يتأكد من وجود البوت والمستخدم في الروم الصوتي المناسب."""

    if ctx.author.voice is None or ctx.author.voice.channel is None:
        await ctx.send("❌ لازم تكون داخل روم صوتي أولاً.")
        return None

    target_channel = ctx.author.voice.channel
    voice_client = ctx.guild.voice_client

    if voice_client is not None and voice_client.is_connected():
        if voice_client.channel.id != target_channel.id:
            state = get_music_state(ctx.guild.id)
            if state.current is not None or state.queue:
                await ctx.send(
                    "❌ البوت يشغل موسيقى في روم صوتي آخر حالياً.",
                )
                return None
            await voice_client.move_to(target_channel)
        return voice_client

    permissions = target_channel.permissions_for(ctx.guild.me)
    if not permissions.connect or not permissions.speak:
        await ctx.send(
            "❌ أحتاج صلاحيتَي **Connect** و **Speak** في هذا الروم.",
        )
        return None

    try:
        return await target_channel.connect()
    except (discord.Forbidden, discord.ClientException) as error:
        logger.exception("تعذر الاتصال بالروم الصوتي.")
        await ctx.send(f"❌ تعذر دخول الروم الصوتي: `{error}`")
        return None


@bot.event
async def on_ready() -> None:
    logger.info("تم تسجيل الدخول بنجاح باسم %s", bot.user)


@bot.command(
    name="play",
    aliases=["تشغيل"],
    help="يشغل أغنية أو يضيفها إلى الطابور.",
)
@commands.guild_only()
async def play(ctx: commands.Context, *, search: str) -> None:
    """!play [اسم الأغنية أو الرابط]"""

    state = get_music_state(ctx.guild.id)
    voice_client = await ensure_voice_connection(ctx)
    if voice_client is None:
        return

    state.stop_requested = False
    state.text_channel = ctx.channel

    async with ctx.typing():
        try:
            track = await extract_track(search, str(ctx.author))
        except Exception as error:
            logger.exception("فشل البحث عن الأغنية.")
            await ctx.send(f"⚠️ ما قدرت أجيب الأغنية: `{error}`")
            return

    state.queue.append(track)
    position = len(state.queue) + (1 if state.current else 0)

    await ctx.send(
        f"✅ تمت إضافة **{track.title}** إلى الطابور "
        f"(المركز: {position}).",
    )

    if state.player_task is None or state.player_task.done():
        state.player_task = asyncio.create_task(
            player_loop(ctx.guild, state),
            name=f"music-player-{ctx.guild.id}",
        )


@bot.command(
    name="pause",
    aliases=["إيقاف", "ايقاف"],
    help="يوقف الأغنية مؤقتاً.",
)
@commands.guild_only()
async def pause(ctx: commands.Context) -> None:
    """!pause"""

    voice_client = ctx.guild.voice_client
    if voice_client and voice_client.is_playing():
        voice_client.pause()
        await ctx.send("⏸️ تم الإيقاف المؤقت.")
    else:
        await ctx.send("❌ لا توجد أغنية تعمل حالياً.")


@bot.command(
    name="resume",
    aliases=["استئناف"],
    help="يستأنف الأغنية المتوقفة مؤقتاً.",
)
@commands.guild_only()
async def resume(ctx: commands.Context) -> None:
    """!resume"""

    voice_client = ctx.guild.voice_client
    if voice_client and voice_client.is_paused():
        voice_client.resume()
        await ctx.send("▶️ تم الاستئناف.")
    else:
        await ctx.send("❌ لا توجد أغنية متوقفة مؤقتاً.")


@bot.command(
    name="skip",
    aliases=["next", "تخطي", "التالي"],
    help="يتخطى الأغنية الحالية.",
)
@commands.guild_only()
async def skip(ctx: commands.Context) -> None:
    """!skip أو !next"""

    state = get_music_state(ctx.guild.id)
    voice_client = ctx.guild.voice_client

    if (
        voice_client is None
        or state.current is None
        or not (voice_client.is_playing() or voice_client.is_paused())
    ):
        await ctx.send("❌ لا توجد أغنية حالية لتخطيها.")
        return

    voice_client.stop()
    await ctx.send(f"⏭️ تم تخطي **{state.current.title}**.")


@bot.command(
    name="stop",
    aliases=["خروج"],
    help="يمسح الطابور ويخرج البوت من الروم الصوتي.",
)
@commands.guild_only()
async def stop(ctx: commands.Context) -> None:
    """!stop"""

    state = get_music_state(ctx.guild.id)
    state.stop_requested = True
    state.queue.clear()

    voice_client = ctx.guild.voice_client
    if voice_client is None:
        await ctx.send("❌ البوت ليس داخل روم صوتي.")
        return

    if voice_client.is_playing() or voice_client.is_paused():
        voice_client.stop()

    if state.player_task and not state.player_task.done():
        state.player_task.cancel()

    try:
        await voice_client.disconnect()
        await ctx.send("👋 تم مسح الطابور والخروج من الروم الصوتي.")
    except (discord.Forbidden, discord.HTTPException) as error:
        await ctx.send(f"⚠️ تعذر الخروج من الروم الصوتي: `{error}`")


@bot.command(
    name="help",
    aliases=["مساعدة"],
    help="يعرض قائمة الأوامر.",
)
async def help_command(ctx: commands.Context) -> None:
    """!help"""

    await ctx.send(
        "**أوامر بوت الموسيقى:**\n"
        "`!play اسم الأغنية أو الرابط` — بحث وتشغيل أو إضافة للطابور\n"
        "`!pause` — إيقاف مؤقت\n"
        "`!resume` — استئناف\n"
        "`!skip` أو `!next` — تخطي الأغنية الحالية\n"
        "`!stop` — مسح الطابور والخروج من الروم\n"
        "`!help` — عرض هذه المساعدة",
    )


@bot.event
async def on_command_error(
    ctx: commands.Context,
    error: commands.CommandError,
) -> None:
    """رسائل أخطاء عربية ومختصرة للأوامر."""

    if isinstance(error, commands.CommandNotFound):
        return

    if isinstance(error, commands.MissingRequiredArgument):
        await ctx.send(
            "❌ ناقص اسم الأغنية أو الرابط.\n"
            "مثال: `!play اسم الأغنية`",
        )
        return

    if isinstance(error, commands.NoPrivateMessage):
        await ctx.send("❌ هذه الأوامر تعمل داخل السيرفر فقط.")
        return

    if isinstance(error, commands.CommandInvokeError):
        logger.exception(
            "حدث خطأ في الأمر %s",
            getattr(ctx.command, "qualified_name", "unknown"),
            exc_info=error.original,
        )
        await ctx.send("⚠️ صار خطأ غير متوقع أثناء تنفيذ الأمر.")
        return

    logger.exception("خطأ غير معالج في الأوامر: %s", error)


def main() -> None:
    token = os.getenv("DISCORD_TOKEN")
    if not token:
        raise RuntimeError(
            "لم يتم العثور على DISCORD_TOKEN. "
            "أضفه إلى Secrets أو إلى ملف .env.",
        )

    bot.run(token)


if __name__ == "__main__":
    main()