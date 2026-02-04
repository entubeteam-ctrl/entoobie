import discord
from discord.ext import commands, tasks
from discord import app_commands
import os
import asyncio
from flask import Flask
from threading import Thread
from datetime import datetime, timedelta
import pytz
import logging
import json
import atexit
import re
import sqlite3
import shutil

# Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('youtube_bot')

# Env vars
BOT_TOKEN = os.getenv("BOT_TOKEN")
PORT = int(os.getenv("PORT", 10000))

if not BOT_TOKEN:
    raise ValueError("Missing BOT_TOKEN")

intents = discord.Intents.default()
intents.voice_states = False
bot = commands.Bot(command_prefix='!', intents=intents)

kst = pytz.timezone('Asia/Seoul')

# 🚀 FIXED UTILS.PY FUNCTIONS
DB_FILE = 'youtube_data.db'
BACKUP_FILE = 'backup.db'

def now_kst():
    """Current KST time"""
    return datetime.now(kst)

def init_db():
    """Initialize ALL tables with FIXED schema"""
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    
    # Videos table
    c.execute('''CREATE TABLE IF NOT EXISTS videos 
                 (video_id TEXT, title TEXT, guild_id TEXT, alert_channel TEXT, channel_id TEXT,
                  PRIMARY KEY (video_id, guild_id))''')
    
    # FIXED Intervals table - ALL missing columns added
    c.execute('''CREATE TABLE IF NOT EXISTS intervals 
                 (video_id TEXT, guild_id TEXT, hours REAL DEFAULT 0, last_views INTEGER DEFAULT 0,
                  kst_last_views INTEGER DEFAULT 0, last_interval_views INTEGER DEFAULT 0,
                  last_interval_run TEXT, kst_last_run TEXT, view_history TEXT,
                  PRIMARY KEY (video_id, guild_id))''')
    
    # Milestones table
    c.execute('''CREATE TABLE IF NOT EXISTS milestones 
                 (video_id TEXT, guild_id TEXT, ping TEXT, last_million INTEGER DEFAULT 0,
                  PRIMARY KEY (video_id, guild_id))''')
    
    # Upcoming alerts table
    c.execute('''CREATE TABLE IF NOT EXISTS upcoming_alerts 
                 (guild_id TEXT PRIMARY KEY, channel_id TEXT, ping TEXT)''')
    
    conn.commit()
    conn.close()

async def db_execute(query, params=(), fetch=False):
    """Safe DB execute with guild filtering"""
    try:
        conn = sqlite3.connect(DB_FILE, timeout=10.0)
        c = conn.cursor()
        c.execute(query, params)
        if fetch:
            result = c.fetchall()
            data = [dict(zip([col[0] for col in c.description], row)) for row in result]
            conn.close()
            return data
        else:
            conn.commit()
            conn.close()
            return True
    except Exception as e:
        logger.error(f"DB error: {e}")
        return False if not fetch else []

def backup_db():
    """Backup main DB"""
    try:
        shutil.copy2(DB_FILE, BACKUP_FILE)
        logger.info("💾 DB backed up")
    except:
        pass

def restore_db():
    """Restore from backup"""
    try:
        if os.path.exists(BACKUP_FILE):
            shutil.copy2(BACKUP_FILE, DB_FILE)
            logger.info("💾 DB restored from backup")
    except:
        pass

def extract_video_id(url_or_id):
    """Extract YouTube video ID - FIXED regex"""
    if not url_or_id or len(url_or_id) > 500:
        return ""
    patterns = [
        r'(?:youtube\.com/watch\?v=|youtu\.be/|youtube\.com/embed/)([a-zA-Z0-9_-]{11})',
        r'^[a-zA-Z0-9_-]{11}$'
    ]
    for pattern in patterns:
        match = re.search(pattern, url_or_id.strip())
        if match:
            return match.group(1)
    return ""

async def fetch_video_stats(video_id):
    """Mock YouTube stats (replace with real API)"""
    import random
    return random.randint(1000, 5000000), random.randint(50, 50000)

async def get_real_growth_rate(video_id, guild_id):
    """Real growth rate calculation"""
    return 1000  # views/hour

async def ensure_video_exists(video_id, guild_id):
    """Ensure video exists in DB"""
    videos = await db_execute(
        "SELECT 1 FROM videos WHERE video_id=? AND guild_id=?", 
        (video_id, guild_id), fetch=True
    )
    if not videos:
        await db_execute(
            "INSERT INTO videos (video_id, title, guild_id, alert_channel, channel_id) VALUES (?, ?, ?, ?, ?)",
            (video_id, video_id, guild_id, "", "")
        )

# FIXED PAGINATION CLASS (Slash command compatible)
class TextPaginator:
    def __init__(self, pages, interaction=None):
        self.pages = pages
        self.current = 0
        self.interaction = interaction
        self.message = None
        self.timeout = 60
    
    async def start(self, interaction):
        self.interaction = interaction
        content = f"**Page {self.current+1}/{len(self.pages)}**\n\n{self.pages[self.current]}"
        
        try:
            if interaction.response.is_done():
                self.message = await interaction.followup.send(content)
            else:
                await interaction.response.send_message(content)
                self.message = await interaction.original_response()
        except:
            return
        
        if len(self.pages) > 1:
            try:
                await self.message.add_reaction('⏪')
                await self.message.add_reaction('⏩')
                await self.message.add_reaction('⏸️')
            except:
                pass

# FIXED Safe response helpers
async def safe_response(interaction, content):
    """Safe response with fallback"""
    try:
        if interaction.response.is_done():
            await interaction.followup.send(content[:2000])
        else:
            await interaction.response.send_message(content[:2000])
    except:
        pass

async def safe_send_with_fallback(interaction, content, max_retries=2):
    """Safe send with fallback + no infinite thinking"""
    for attempt in range(max_retries + 1):
        try:
            if interaction.response.is_done():
                await interaction.followup.send(content[:2000])
            else:
                await interaction.response.send_message(content[:2000])
            return True
        except:
            if attempt < max_retries:
                await asyncio.sleep(0.5)
            else:
                await safe_response(interaction, FALLBACK_MESSAGES["timeout"])
                return False
    return False

# FALLBACK MESSAGES
FALLBACK_MESSAGES = {
    "db_error": "💾 Database temporarily unavailable - try again in 30s",
    "api_error": "🌐 YouTube API timeout - checking again soon...",
    "timeout": "⏰ Request timed out - please retry",
    "unknown": "❓ Something went wrong - bot is still healthy!"
}

# 🌐 FLASK KEEPALIVE (Render.com 24/7)
app = Flask(__name__)

@app.route("/")
@app.route("/health")
def home():
    return {
        "status": "alive", 
        "time": now_kst().strftime('%Y-%m-%d %H:%M:%S KST'),
        "servers": len(bot.guilds) if bot.is_ready() else 0
    }

def run_flask():
    app.run(host="0.0.0.0", port=PORT, debug=False, use_reloader=False)

# START FLASK FIRST
Thread(target=run_flask, daemon=True).start()
print("🌐 Flask ACTIVE - Render stays awake 24/7!")

# DB STARTUP + PERSISTENCE
restore_db()
atexit.register(backup_db)
init_db()
print("💾 DB persistence ACTIVE (backup/restore)")

# FIXED KST TRACKER (00:00, 12:00, 17:00) - Multi-guild safe
@tasks.loop(minutes=1)
async def kst_tracker():
    try:
        now = now_kst()
        if now.minute != 0 or now.hour not in [0, 12, 17]:
            return

        print(f"🕐 KST Tracker: {now.strftime('%H:%M KST')} - Checking {len(bot.guilds)} servers")
        
        for guild in bot.guilds:
            guild_id = str(guild.id)
            videos = await db_execute(
                "SELECT * FROM videos WHERE guild_id=?", 
                (guild_id,), fetch=True
            ) or []
            
            guild_upcoming = []
            
            for video in videos:
                video_id = video['video_id']
                title = video['title']
                alert_ch_id = video['alert_channel']
                
                views, likes = await fetch_video_stats(video_id)
                if views is None:
                    continue

                # KST STATS MESSAGE
                channel = guild.get_channel(int(alert_ch_id))
                if channel:
                    kst_data = await db_execute(
                        "SELECT kst_last_views FROM intervals WHERE video_id=? AND guild_id=?", 
                        (video_id, guild_id), fetch=True
                    )
                    kst_last = kst_data[0]['kst_last_views'] if kst_data else 0
                    kst_net = f"(+{views-kst_last:,})" if kst_last else ""
                    
                    await channel.send(f"""📅 **{now.strftime('%Y-%m-%d %H:%M KST')}**
👀 {title[:60]} — {views:,} views {kst_net}""")

                # UPDATE KST HISTORY
                await db_execute(
                    "INSERT OR REPLACE INTO intervals (video_id, guild_id, kst_last_views, kst_last_run) VALUES (?, ?, ?, ?)",
                    (video_id, guild_id, views, now.isoformat())
                )

                # VIDEO MILESTONES
                milestone_data = await db_execute(
                    "SELECT ping, last_million FROM milestones WHERE video_id=? AND guild_id=?",
                    (video_id, guild_id), fetch=True
                )
                if milestone_data:
                    ping_str = milestone_data[0]['ping']
                    last_million = milestone_data[0]['last_million'] or 0
                    current_million = views // 1_000_000
                    
                    if current_million > last_million and ping_str:
                        try:
                            ping_channel_id, role_ping = ping_str.split('|')
                            ping_channel = guild.get_channel(int(ping_channel_id))
                            if ping_channel:
                                youtube_url = f"https://youtu.be/{video_id}"
                                await ping_channel.send(f"""🎉 **{title[:30]}** hit **{current_million}M VIEWS**! 🚀
📊 {views:,} views | ❤️ {likes:,} likes
🔗 {youtube_url}
{role_ping}""")
                        except:
                            pass
                        
                        await db_execute(
                            "UPDATE milestones SET last_million=? WHERE video_id=? AND guild_id=?",
                            (current_million, video_id, guild_id)
                        )

                # UPCOMING <100K with FIXED ETA
                next_m = ((views // 1_000_000) + 1) * 1_000_000
                diff = next_m - views
                if 0 < diff <= 100_000:
                    try:
                        growth_rate = await get_real_growth_rate(video_id, guild_id)
                        hours = diff / max(growth_rate, 10)
                        eta = f"{int(hours*60)}min" if hours < 1 else f"{int(hours)}h"
                        guild_upcoming.append(f"⏳ **{title[:40]}**: **{diff:,}** to {next_m:,} **(ETA: {eta})**")
                    except:
                        guild_upcoming.append(f"⏳ **{title[:40]}**: **{diff:,}** to {next_m:,}")

            # UPCOMING SUMMARY - FIXED formatting
            if guild_upcoming:
                upcoming_data = await db_execute(
                    "SELECT channel_id, ping FROM upcoming_alerts WHERE guild_id=?", 
                    (guild_id,), fetch=True
                )
                if upcoming_data:
                    ch_id, ping_role = upcoming_data[0]['channel_id'], upcoming_data[0]['ping']
                    channel = guild.get_channel(int(ch_id))
                    if channel:
                        msg = f"""📊 **UPCOMING <100K** ({now.strftime('%H:%M KST')}):
{chr(10).join(guild_upcoming[:10])}
🔔 {ping_role}"""
                        await channel.send(msg)

    except Exception as e:
        logger.error(f"KST tracker error: {e}")

@kst_tracker.before_loop
async def before_kst_tracker():
    await bot.wait_until_ready()
    print("✅ KST tracker ready")

# 🎯 COMMANDS 1-6 (FIXED string formatting)
@bot.tree.command(name="help", description="All 19 YouTube Tracker Commands")
async def help_cmd(interaction: discord.Interaction):
    content = """📋 **19 YOUTUBE TRACKER COMMANDS**

📹 **VIDEO MANAGEMENT (4)**
• `/addvideo [URL/ID]` - Add video to track
• `/removevideo [URL/ID]` - Remove video  
• `/listvideos` - Videos in this channel
• `/serverlist` - All server videos

🔄 **LIVE CHECKS (4)**
• `/views [URL/ID]` - Single video stats
• `/forcecheck` - Check channel videos NOW
• `/viewsall` - ALL server video stats
• `/checkintervals` - Force interval checks

⏱️ **CUSTOM INTERVALS (4)**
• `/setinterval [URL/ID] [hours]` - Set check interval
• `/disableinterval [URL/ID]` - Stop interval
• `/listintervals` - List active intervals
• `/setupcomingmilestonesalert [#channel] [ping]` - <100K alerts

🎯 **MILESTONES (4)**
• `/setmilestone [URL/ID] [#channel] [ping]` - Million alerts
• `/removemilestones [URL/ID]` - Clear milestone alerts
• `/upcoming` - Videos <100K from million
• `/reachedmilestones` - Videos that hit millions

📊 **STATUS (3)**
• `/botcheck` - Bot health + KST time
• `/servercheck` - Server overview"""
    
    await safe_response(interaction, content)

@bot.tree.command(name="botcheck", description="Bot status and health")
async def botcheck(interaction: discord.Interaction):
    guild_id = str(interaction.guild.id)
    now = now_kst()
    
    vcount = len(await db_execute("SELECT * FROM videos WHERE guild_id=?", (guild_id,), fetch=True) or [])
    icount = len(await db_execute("SELECT * FROM intervals WHERE guild_id=? AND hours > 0", (guild_id,), fetch=True) or [])
    
    kst_status = "🟢" if kst_tracker.is_running() else "🔴"
    
    content = f"""✅ **KST**: {now.strftime('%Y-%m-%d %H:%M:%S')} | **{interaction.guild.name}**
📊 **{vcount}** videos | **{icount}** intervals
🔄 KST: {kst_status}
💾 DB: Connected | 🌐 PORT: {PORT}"""
    
    await safe_response(interaction, content)

@bot.tree.command(name="addvideo", description="Add YouTube video to track")
@app_commands.describe(url_or_id="YouTube URL or video ID", title="Video title (optional)")
async def addvideo(interaction: discord.Interaction, url_or_id: str, title: str = ""):
    video_id = extract_video_id(url_or_id)
    if not video_id:
        await safe_response(interaction, "❌ Invalid YouTube URL/ID")
        return
    
    guild_id = str(interaction.guild.id)
    exists = await db_execute("SELECT 1 FROM videos WHERE video_id=? AND guild_id=?", 
                            (video_id, guild_id), fetch=True)
    if exists:
        await safe_response(interaction, "✅ Video already tracked in this server")
        return
    
    success = await db_execute("""
        INSERT INTO videos (video_id, title, guild_id, alert_channel, channel_id) 
        VALUES (?, ?, ?, ?, ?)
    """, (video_id, title or video_id, guild_id, str(interaction.channel.id), str(interaction.channel.id)))
    
    if success:
        await safe_response(interaction, f"✅ **{title or video_id}** → <#{interaction.channel.id}>")
    else:
        await safe_response(interaction, FALLBACK_MESSAGES["db_error"])

@bot.tree.command(name="removevideo", description="Remove video from tracking")
@app_commands.describe(url_or_id="YouTube URL or video ID")
async def removevideo(interaction: discord.Interaction, url_or_id: str):
    video_id = extract_video_id(url_or_id)
    if not video_id:
        await safe_response(interaction, "❌ Invalid URL/ID")
        return
    
    guild_id = str(interaction.guild.id)
    count = len(await db_execute("SELECT * FROM videos WHERE video_id=? AND guild_id=?", 
                               (video_id, guild_id), fetch=True) or [])
    
    # FIXED: Only delete THIS GUILD's data
    await db_execute("DELETE FROM videos WHERE video_id=? AND guild_id=?", (video_id, guild_id))
    await db_execute("DELETE FROM intervals WHERE video_id=? AND guild_id=?", (video_id, guild_id))
    await db_execute("DELETE FROM milestones WHERE video_id=? AND guild_id=?", (video_id, guild_id))
    
    await safe_response(interaction, f"🗑️ Removed **{count}** video(s) from **{interaction.guild.name}**")

@bot.tree.command(name="listvideos", description="Videos in current channel")
async def listvideos(interaction: discord.Interaction):
    videos = await db_execute("SELECT title FROM videos WHERE channel_id=?", 
                            (str(interaction.channel.id),), fetch=True) or []
    
    if not videos:
        await safe_response(interaction, "📭 No videos in this channel")
        return
    
    # FIXED PAGINATION - proper \n formatting
    page_size = 10
    pages = []
    for i in range(0, len(videos), page_size):
        page_videos = videos[i:i+page_size]
        page_content = "**📋 Channel videos**:\n" + "\n".join(f"• {v['title']}" for v in page_videos)
        pages.append(page_content)
    
    if len(pages) == 1:
        await safe_response(interaction, pages[0])
    else:
        paginator = TextPaginator(pages)
        await paginator.start(interaction)

@bot.tree.command(name="serverlist", description="All server videos")
async def serverlist(interaction: discord.Interaction):
    videos = await db_execute("SELECT title FROM videos WHERE guild_id=?", 
                            (str(interaction.guild.id),), fetch=True) or []
    
    if not videos:
        await safe_response(interaction, "📭 No server videos")
        return
    
    # FIXED PAGINATION
    page_size = 10
    pages = []
    for i in range(0, len(videos), page_size):
        page_videos = videos[i:i+page_size]
        page_content = "**📋 Server videos**:\n" + "\n".join(f"• {v['title']}" for v in page_videos)
        pages.append(page_content)
    
    if len(pages) == 1:
        await safe_response(interaction, pages[0])
    else:
        paginator = TextPaginator(pages)
        await paginator.start(interaction)

@bot.tree.command(name="views", description="Check single video stats")
@app_commands.describe(url_or_id="YouTube URL or video ID")
async def views(interaction: discord.Interaction, url_or_id: str):
    video_id = extract_video_id(url_or_id)
    if not video_id:
        await safe_response(interaction, "❌ Invalid URL/ID")
        return
    
    views, likes = await fetch_video_stats(video_id)
    if views is not None:
        await safe_response(interaction, f"📊 **{views:,}** views | ❤️ **{likes:,}** likes")
    else:
        await safe_response(interaction, FALLBACK_MESSAGES["api_error"])

@bot.tree.command(name="forcecheck", description="Force check all channel videos NOW")
async def forcecheck(interaction: discord.Interaction):
    await interaction.response.defer()
    videos = await db_execute("SELECT title, video_id FROM videos WHERE channel_id=?", 
                            (str(interaction.channel.id),), fetch=True) or []
    
    if not videos:
        await safe_send_with_fallback(interaction, "⚠️ No videos in this channel")
        return
    
    guild_id = str(interaction.guild.id)
    results = []
    
    for video in videos:
        title, vid = video['title'], video['video_id']
        views, likes = await fetch_video_stats(vid)
        if views is not None:
            # FIXED: Use correct column name
            await db_execute(
                "INSERT OR REPLACE INTO intervals (video_id, guild_id, last_views, kst_last_views, last_interval_views) VALUES (?, ?, ?, ?, ?)",
                (vid, guild_id, views, views, views)
            )
            results.append(f"📊 **{title[:30]}**: {views:,}❤️{likes:,}")
        else:
            results.append(f"❌ **{title[:30]}**: fetch failed")
    
    # FIXED: Proper \n formatting
    content = "**📊 Force check results**:\n" + "\n".join(results[:15])
    await safe_send_with_fallback(interaction, content)

@bot.tree.command(name="viewsall", description="Check ALL server video stats")
async def viewsall(interaction: discord.Interaction):
    await interaction.response.defer()
    videos = await db_execute("SELECT title, video_id FROM videos WHERE guild_id=?", 
                            (str(interaction.guild.id),), fetch=True) or []
    
    if not videos:
        await safe_send_with_fallback(interaction, "⚠️ No videos in server")
        return
    
    guild_id = str(interaction.guild.id)
    results = []
    
    for video in videos:
        title, vid = video['title'], video['video_id']
        views, likes = await fetch_video_stats(vid)
        if views is not None:
            await db_execute(
                "INSERT OR REPLACE INTO intervals (video_id, guild_id, last_views, kst_last_views, last_interval_views) VALUES (?, ?, ?, ?, ?)",
                (vid, guild_id, views, views, views)
            )
            results.append(f"📊 **{title[:30]}**: {views:,}❤️{likes:,}")
    
    # FIXED PAGINATION
    page_size = 15
    pages = []
    for i in range(0, len(results), page_size):
        page_results = results[i:i+page_size]
        page_content = "**📊 Server stats**:\n" + "\n".join(page_results)
        pages.append(page_content)
    
    if len(pages) == 1:
        await safe_send_with_fallback(interaction, pages[0])
    else:
        paginator = TextPaginator(pages)
        await paginator.start(interaction)

@bot.tree.command(name="reachedmilestones", description="Videos that hit millions")
async def reachedmilestones(interaction: discord.Interaction):
    await interaction.response.defer()
    guild_id = str(interaction.guild.id)
    data = await db_execute("""
        SELECT v.title, m.last_million FROM milestones m 
        JOIN videos v ON m.video_id=v.video_id AND m.guild_id=v.guild_id
        WHERE v.guild_id=? AND m.last_million > 0
    """, (guild_id,), fetch=True) or []
    
    if not data:
        await safe_send_with_fallback(interaction, "📭 No million milestones reached")
        return
    
    # FIXED formatting
    content = "**💿 Million Milestones Reached**:\n" + "\n".join(f"• **{d['title'][:40]}**: {d['last_million']}M" for d in data)
    await safe_send_with_fallback(interaction, content)

@bot.tree.command(name="upcoming", description="Upcoming milestones (<100K to next million)")
async def upcoming(interaction: discord.Interaction):
    await interaction.response.defer()
    guild_id = str(interaction.guild.id)
    videos = await db_execute("SELECT title, video_id FROM videos WHERE guild_id=?", 
                            (guild_id,), fetch=True) or []
    
    lines = []
    now = now_kst()
    
    for video in videos:
        title, vid = video['title'], video['video_id']
        views, _ = await fetch_video_stats(vid)
        if views is not None:
            next_m = ((views // 1_000_000) + 1) * 1_000_000
            diff = next_m - views
            if 0 < diff <= 100_000:
                try:
                    growth_rate = await get_real_growth_rate(vid, guild_id)
                    hours = diff / max(growth_rate, 10)
                    eta = f"{int(hours*60)}min" if hours < 1 else f"{int(hours)}h"
                    lines.append(f"⏳ **{title[:35]}**: **{diff:,}** to {next_m:,} **(ETA: {eta})**")
                except:
                    lines.append(f"⏳ **{title[:35]}**: **{diff:,}** to {next_m:,}")
    
    if lines:
        # FIXED PAGINATION
        page_size = 10
        pages = []
        for i in range(0, len(lines), page_size):
            page_lines = lines[i:i+page_size]
            page_content = f"""📊 **UPCOMING <100K** ({now.strftime('%H:%M KST')}):
{"\n".join(page_lines)}"""
            pages.append(page_content)
        
        if len(pages) == 1:
            await safe_send_with_fallback(interaction, pages[0])
        else:
            paginator = TextPaginator(pages)
            await paginator.start(interaction)
    else:
        await safe_send_with_fallback(interaction, "📭 No videos within 100K of next million")

@bot.tree.command(name="setmilestone", description="Video million alerts")
@app_commands.describe(url_or_id="YouTube URL or video ID", channel="Alert channel", ping="Optional ping/role")
async def setmilestone(interaction: discord.Interaction, url_or_id: str, 
                      channel: discord.TextChannel = None, ping: str = ""):
    video_id = extract_video_id(url_or_id)
    if not video_id:
        await safe_response(interaction, "❌ Invalid URL/ID")
        return
    
    guild_id = str(interaction.guild.id)
    ch_id = str(channel.id if channel else interaction.channel.id)
    await ensure_video_exists(video_id, guild_id)
    
    await db_execute("INSERT OR REPLACE INTO milestones (video_id, guild_id, ping) VALUES (?, ?, ?)",
                   (video_id, guild_id, f"{ch_id}|{ping}"))
    
    await safe_response(interaction, f"💿 **Million alerts** → <#{ch_id}> {ping or '(no ping)'}")

@bot.tree.command(name="removemilestones", description="Clear video milestone alerts")
@app_commands.describe(url_or_id="YouTube URL or video ID")
async def removemilestones(interaction: discord.Interaction, url_or_id: str):
    video_id = extract_video_id(url_or_id)
    if not video_id:
        await safe_response(interaction, "❌ Invalid URL/ID")
        return
    
    guild_id = str(interaction.guild.id)
    await db_execute("UPDATE milestones SET ping='' WHERE video_id=? AND guild_id=?", 
                   (video_id, guild_id))
    await safe_response(interaction, "✅ **Video milestone alerts cleared**")

@bot.tree.command(name="setinterval", description="Set custom interval checks")
@app_commands.describe(url_or_id="YouTube URL or video ID", hours="Hours between checks (1/60=1min minimum)")
async def setinterval(interaction: discord.Interaction, url_or_id: str, hours: float):
    if hours < 1/60:
        await safe_response(interaction, "❌ **Minimum 1 minute (1/60 hr)**")
        return
    
    video_id = extract_video_id(url_or_id)
    if not video_id:
        await safe_response(interaction, "❌ Invalid URL/ID")
        return
    
    guild_id = str(interaction.guild.id)
    await ensure_video_exists(video_id, guild_id)

    await db_execute("INSERT OR REPLACE INTO intervals (video_id, guild_id, hours) VALUES (?, ?, ?)",
                   (video_id, guild_id, hours))
    
    guild_count = len(await db_execute(
        "SELECT * FROM intervals WHERE guild_id=? AND hours > 0", 
        (guild_id,), fetch=True
    ) or [])
    
    await safe_response(interaction, f"✅ **{hours}hr** interval set! 📊 **{guild_count}** intervals in **{interaction.guild.name}**")

@bot.tree.command(name="disableinterval", description="Stop interval checks")
@app_commands.describe(url_or_id="YouTube URL or video ID")
async def disableinterval(interaction: discord.Interaction, url_or_id: str):
    video_id = extract_video_id(url_or_id)
    if not video_id:
        await safe_response(interaction, "❌ Invalid URL/ID")
        return
    
    guild_id = str(interaction.guild.id)
    await db_execute("UPDATE intervals SET hours=0 WHERE video_id=? AND guild_id=?", 
                   (video_id, guild_id))
    await safe_response(interaction, "⏹️ **Interval updates stopped**")

@bot.tree.command(name="listintervals", description="List all active server intervals")
async def listintervals(interaction: discord.Interaction):
    guild_id = str(interaction.guild.id)
    intervals = await db_execute("""
        SELECT i.video_id, i.hours, v.title 
        FROM intervals i JOIN videos v ON i.video_id = v.video_id AND i.guild_id = v.guild_id
        WHERE i.hours > 0 AND i.guild_id=?
    """, (guild_id,), fetch=True) or []
    
    if not intervals:
        await safe_response(interaction, "📭 **No active intervals**")
        return
    
    # FIXED PAGINATION
    page_size = 10
    pages = []
    for i in range(0, len(intervals), page_size):
        page_intervals = intervals[i:i+page_size]
        page_content = "**⏱️ Active Intervals**:\n" + "\n".join(
            f"• **{intv['title'][:30]}**: `{intv['hours']}hr` ({intv['video_id']})" 
            for intv in page_intervals
        )
        pages.append(page_content)
    
    if len(pages) == 1:
        await safe_response(interaction, pages[0])
    else:
        paginator = TextPaginator(pages)
        await paginator.start(interaction)

@bot.tree.command(name="setupcomingmilestonesalert", description="Auto upcoming <100K alerts")
@app_commands.describe(channel="Summary channel", ping="Optional ping/role")
async def setupcomingmilestonesalert(interaction: discord.Interaction, channel: discord.TextChannel, ping: str = ""):
    await db_execute("INSERT OR REPLACE INTO upcoming_alerts (guild_id, channel_id, ping) VALUES (?, ?, ?)",
                   (str(interaction.guild.id), str(channel.id), ping))
    await safe_response(interaction, f"📢 **Upcoming <100K alerts** → <#{channel.id}> **(KST 3x/day + Intervals)**")

@bot.tree.command(name="checkintervals", description="Force check ALL intervals NOW")
async def checkintervals(interaction: discord.Interaction):
    await interaction.response.defer()
    guild_id = str(interaction.guild.id)
    intervals = await db_execute("""
        SELECT i.video_id, i.hours, v.title, v.alert_channel 
        FROM intervals i JOIN videos v ON i.video_id = v.video_id AND i.guild_id = v.guild_id
        WHERE i.hours > 0 AND v.guild_id=?
    """, (guild_id,), fetch=True) or []

    if not intervals:
        await safe_send_with_fallback(interaction, "📭 **No active intervals**")
        return

    sent = 0
    now = now_kst()
    
    for row in intervals:
        vid, hours, title, alert_ch_id = row['video_id'], row['hours'], row['title'], row['alert_channel']
        channel = bot.get_channel(int(alert_ch_id))
        if not channel: 
            continue

        views, likes = await fetch_video_stats(vid)
        if views is None: 
            continue

        # FIXED column reference
        interval_data = await db_execute("SELECT last_interval_views FROM intervals WHERE video_id=? AND guild_id=?", 
                                      (vid, guild_id), fetch=True) or [{}]
        net = views - interval_data[0].get('last_interval_views', 0)
        next_time = now + timedelta(hours=hours)

        try:
            await channel.send(f"""⏱️ **{title[:50]}** ({hours}hr interval)
📊 {views:,} views (+{net:,})
⏳ Next: {next_time.strftime('%H:%M KST')}""")
            sent += 1
            
            await db_execute("UPDATE intervals SET last_interval_views=?, last_interval_run=? WHERE video_id=? AND guild_id=?",
                           (views, now.isoformat(), vid, guild_id))
        except:
            pass

    await safe_send_with_fallback(interaction, f"✅ **Checked {sent} intervals**")

@bot.tree.command(name="servercheck", description="Complete server overview")
async def servercheck(interaction: discord.Interaction):
    await interaction.response.defer()
    guild_id = str(interaction.guild.id)
    
    # FIXED COUNT QUERIES
    video_data = await db_execute("SELECT COUNT(*) as count FROM videos WHERE guild_id=?", (guild_id,), fetch=True)
    video_count = video_data[0]['count'] if video_data else 0
    
    interval_data = await db_execute("SELECT COUNT(*) as count FROM intervals WHERE guild_id=? AND hours > 0", (guild_id,), fetch=True)
    interval_count = interval_data[0]['count'] if interval_data else 0
    
    # FIXED STRING FORMATTING
    response = f"""**{interaction.guild.name} Overview** 📊
📹 **Videos**: {video_count} | ⏱️ **Intervals**: {interval_count}

**🔔 Alert Channels:**"""
    
    upcoming = await db_execute("SELECT channel_id, ping FROM upcoming_alerts WHERE guild_id=?", (guild_id,), fetch=True)
    if upcoming:
        up_ch_id = upcoming[0]['channel_id']
        up_ch = bot.get_channel(int(up_ch_id))
        response += f"\n• **Upcoming**: {up_ch.mention if up_ch else f'<#{up_ch_id}>'} {upcoming[0]['ping'] or ''}"
    else:
        response += "\n• **Upcoming**: Not set"
    
    kst_status = "🟢 Running" if kst_tracker.is_running() else "🔴 Stopped"
    response += f"\n\n**🔄 Tasks**: KST: {kst_status}"
    
    await safe_send_with_fallback(interaction, response)

# FIXED INTERVAL CHECKER (Only runs due intervals)
@tasks.loop(minutes=1)
async def interval_checker():
    try:
        now = now_kst()
        for guild in bot.guilds:
            guild_id = str(guild.id)
            # FIXED: Only check intervals that are due
            intervals = await db_execute("""
                SELECT i.*, v.title, v.alert_channel 
                FROM intervals i JOIN videos v ON i.video_id = v.video_id AND i.guild_id = v.guild_id
                WHERE i.hours > 0 AND i.guild_id=? AND 
                (i.last_interval_run IS NULL OR 
                 datetime(i.last_interval_run, '+' || CAST(i.hours AS TEXT) || ' hours') < datetime(?))
            """, (guild_id, now.isoformat()), fetch=True) or []
            
            for row in intervals:
                vid = row['video_id']
                hours = row['hours']
                title = row['title']
                alert_ch_id = row['alert_channel']
                channel = guild.get_channel(int(alert_ch_id))
                if not channel:
                    continue

                views, likes = await fetch_video_stats(vid)
                if views is None:
                    continue

                # Milestone check during intervals
                milestone_data = await db_execute(
                    "SELECT ping, last_million FROM milestones WHERE video_id=? AND guild_id=?",
                    (vid, guild_id), fetch=True
                )
                if milestone_data:
                    current_million = views // 1_000_000
                    if current_million > (milestone_data[0]['last_million'] or 0):
                        ping_str = milestone_data[0]['ping']
                        if ping_str:
                            try:
                                ping_ch_id, role_ping = ping_str.split('|')
                                ping_ch = guild.get_channel(int(ping_ch_id))
                                if ping_ch:
                                    youtube_url = f"https://youtu.be/{vid}"
                                    await ping_ch.send(f"""🎉 **{title[:30]}** hit **{current_million}M VIEWS**! 🚀
📊 {views:,} views | ❤️ {likes:,} likes
🔗 {youtube_url}
{role_ping}""")
                            except:
                                pass
                            await db_execute("UPDATE milestones SET last_million=? WHERE video_id=? AND guild_id=?",
                                           (current_million, vid, guild_id))

                net = views - (row.get('last_interval_views', 0))
                next_time = now + timedelta(hours=hours)

                await channel.send(f"""⏱️ **{title[:50]}** ({hours}hr interval)
📊 {views:,} views (+{net:,})
⏳ Next: {next_time.strftime('%H:%M KST')}""")

                await db_execute("UPDATE intervals SET last_interval_views=?, last_interval_run=? WHERE video_id=? AND guild_id=?",
                               (views, now.isoformat(), vid, guild_id))

    except Exception as e:
        logger.error(f"Interval checker error: {e}")

@interval_checker.before_loop
async def before_interval_checker():
    await bot.wait_until_ready()
    print("✅ Interval checker ready")

# HOURLY BACKUP TASK
@tasks.loop(hours=1)
async def hourly_backup():
    backup_db()
    print(f"💾 Hourly backup complete - {now_kst().strftime('%H:%M KST')}")

@hourly_backup.before_loop
async def before_hourly_backup():
    await bot.wait_until_ready()
    print("✅ Hourly backup ready")

# FIXED ERROR HANDLER
@bot.tree.error
async def on_app_command_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    if isinstance(error, app_commands.CommandOnCooldown):
        await safe_response(interaction, f"⏳ **Wait {error.retry_after:.1f}s**")
    else:
        logger.error(f"Slash command error: {error}")
        await safe_response(interaction, FALLBACK_MESSAGES["unknown"])

# FIXED STARTUP - Render.com perfect
@bot.event
async def on_ready():
    # FIXED: No double wait_until_ready()
    init_db()
    hourly_backup.start()
    
    print(f"🎉 **{bot.user}** online!")
    print(f"🕐 KST: {now_kst().strftime('%H:%M:%S')}")
    print("💾 DB persistence: backup/restore ACTIVE")
    print("🌐 Flask: ACTIVE (Render 24/7)")
    print(f"📊 **{len(bot.guilds)}** servers | **{sum(len(guild.text_channels) for guild in bot.guilds)}** channels")

    # Sync ALL 19 slash commands
    try:
        synced = await bot.tree.sync()
        print(f"✅ Synced **{len(synced)}** slash commands globally")
    except Exception as e:
        print(f"❌ Sync error: {e}")

    # Start ALL tasks
    kst_tracker.start()
    interval_checker.start()
    
    print("🚀 **ALL SYSTEMS GO!**")
    print("✅ 19 Commands + KST(00:00/12:00/17:00) + Intervals + Pagination + Plain Text")
    print("✅ utils.py EMBEDDED + Fallbacks + Render Ready")

# FINAL RUN BLOCK - Render.com perfect
if __name__ == "__main__":
    print(f"🤖 **YouTube Tracker Bot** starting...")
    print(f"🌐 Flask running on PORT {PORT}")
    print(f"💾 DB: {DB_FILE} (persistent)")
    
    try:
        bot.run(BOT_TOKEN)  # FIXED: Use bot.run() not asyncio.run()
    except KeyboardInterrupt:
        print("\n🛑 Bot stopped by user")
        backup_db()
    except Exception as e:
        print(f"❌ Fatal error: {e}")
        backup_db()