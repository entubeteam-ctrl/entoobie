# main.py - PART 1/5: Imports + Core Utils
import discord
from discord.ext import commands, tasks
from discord import app_commands
import os
import asyncio
import aiohttp
import aiosqlite
import pytz
from datetime import datetime, timedelta
from flask import Flask
import threading
import re
import json
import logging

# CONFIG
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
TOKEN = os.getenv('BOT_TOKEN')
YT_API_KEY = os.getenv('YOUTUBE_API_KEY')
PORT = int(os.getenv('PORT', 10000))
DB_PATH = "youtube_bot.db"
KST = pytz.timezone('Asia/Seoul')

intents = discord.Intents.default()
bot = commands.Bot(command_prefix='!', intents=intents)

# Flask Keepalive
app = Flask(__name__)
@app.route("/") 
@app.route("/health")
def health():
    return {"status": "alive", "servers": len(bot.guilds) if bot.is_ready() else 0}

def run_flask():
    app.run(host="0.0.0.0", port=PORT, debug=False, use_reloader=False)

# UTILS (EMBEDDED)
async def safe_db(query, params=(), fetch=False):
    """Safe DB with 3x retry."""
    for attempt in range(3):
        try:
            async with aiosqlite.connect(DB_PATH, timeout=10.0) as db:
                if fetch:
                    async with db.execute(query, params) as cur:
                        return await cur.fetchall()
                else:
                    await db.execute(query, params)
                    await db.commit()
                    return True
        except Exception as e:
            if attempt == 2:
                logger.error(f"DB Error: {e}")
                return False
            await asyncio.sleep(0.1)
    return False

def safe_extract_video_id(url_or_id: str) -> str:
    """Bulletproof YouTube ID extraction."""
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

async def safe_yt_stats(video_id: str):
    """Safe YouTube API with timeout."""
    if not YT_API_KEY or not video_id:
        return None, None, ""
    try:
        params = {'id': video_id, 'key': YT_API_KEY, 'part': 'statistics,snippet'}
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as session:
            async with session.get("https://www.googleapis.com/youtube/v3/videos", params=params) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if data.get('items'):
                        item = data['items'][0]
                        stats = item['statistics']
                        title = item['snippet']['title'][:200]
                        return (
                            int(stats.get('viewCount', 0)),
                            int(stats.get('likeCount', 0)),
                            title
                        )
    except:
        pass
    return None, None, ""

async def init_db():
    """Initialize all tables."""
    await safe_db("""
        CREATE TABLE IF NOT EXISTS guild_videos (
            guild_id TEXT, video_id TEXT, title TEXT, channel_id TEXT, 
            PRIMARY KEY (guild_id, video_id)
        )
    """)
    await safe_db("""
        CREATE TABLE IF NOT EXISTS video_intervals (
            guild_id TEXT, video_id TEXT, minutes INTEGER, next_check TEXT,
            PRIMARY KEY (guild_id, video_id)
        )
    """)
    await safe_db("""
        CREATE TABLE IF NOT EXISTS video_milestones (
            guild_id TEXT, video_id TEXT, target INTEGER, alert_channel TEXT, ping TEXT,
            PRIMARY KEY (guild_id, video_id)
        )
    """)
    await safe_db("""
        CREATE TABLE IF NOT EXISTS guild_settings (
            guild_id TEXT PRIMARY KEY, upcoming_channel TEXT, upcoming_ping TEXT
        )
    """)

async def safe_send(interaction, content=None, embed=None, ephemeral=False):
    """Universal safe response."""
    try:
        if interaction.response.is_done():
            return await interaction.followup.send(content, embed=embed, ephemeral=ephemeral)
        else:
            return await interaction.response.send_message(content, embed=embed, ephemeral=ephemeral)
    except:
        try:
            await interaction.followup.send("❌ Response failed", ephemeral=True)
        except:
            pass

# main.py - PART 2/5: Error Handler + Commands 1-6
@bot.tree.error
async def on_app_command_error(interaction: discord.Interaction, error: app_commands.AppCommandError):
    if isinstance(error, app_commands.CommandOnCooldown):
        await safe_send(interaction, f"⏳ Wait {error.retry_after:.1f}s", ephemeral=True)
    else:
        logger.error(f"Command error: {error}")
        await safe_send(interaction, "❌ Command failed - try again", ephemeral=True)

# === COMMANDS 1-6 ===
@bot.tree.command(name="botcheck", description="Bot health check")
async def botcheck(interaction: discord.Interaction):
    try:
        await interaction.response.defer(ephemeral=True)
        guild_count = len(bot.guilds)
        
        embed = discord.Embed(title="🤖 Bot Status", color=0x00ff00)
        embed.add_field(name="Servers", value=guild_count, inline=True)
        embed.add_field(name="Status", value="🟢 Healthy", inline=True)
        embed.add_field(name="Latency", value=f"{bot.latency*1000:.0f}ms", inline=True)
        await safe_send(interaction, embed=embed)
    except Exception as e:
        logger.error(f"botcheck error: {e}")
        await safe_send(interaction, "❌ Botcheck failed", ephemeral=True)

@bot.tree.command(name="addvideo", description="Add video to track")
@app_commands.describe(video="YouTube URL/ID", title="Custom title (optional)")
async def addvideo(interaction: discord.Interaction, video: str, title: str = None):
    try:
        await interaction.response.defer()
        vid_id = safe_extract_video_id(video)
        if not vid_id:
            return await safe_send(interaction, "❌ Invalid YouTube URL/ID", ephemeral=True)
        
        display_title = title or vid_id
        if not title:
            _, _, yt_title = await safe_yt_stats(vid_id)
            display_title = yt_title or vid_id
        
        guild_id = str(interaction.guild.id)
        success = await safe_db("""
            INSERT OR REPLACE INTO guild_videos 
            (guild_id, video_id, title, channel_id) 
            VALUES (?, ?, ?, ?)
        """, (guild_id, vid_id, display_title, str(interaction.channel.id)))
        
        if not success:
            return await safe_send(interaction, "❌ Database error", ephemeral=True)
            
        await safe_send(interaction, f"✅ **{display_title}** → {interaction.channel.mention}")
    except Exception as e:
        logger.error(f"addvideo error: {e}")
        await safe_send(interaction, "❌ Add video failed", ephemeral=True)

@bot.tree.command(name="removevideo", description="Remove video from server")
@app_commands.describe(video="YouTube URL/ID")
async def removevideo(interaction: discord.Interaction, video: str):
    try:
        await interaction.response.defer()
        vid_id = safe_extract_video_id(video)
        if not vid_id:
            return await safe_send(interaction, "❌ Invalid video ID", ephemeral=True)
        
        guild_id = str(interaction.guild.id)
        await safe_db("DELETE FROM guild_videos WHERE guild_id=? AND video_id=?", (guild_id, vid_id))
        await safe_db("DELETE FROM video_intervals WHERE guild_id=? AND video_id=?", (guild_id, vid_id))
        await safe_db("DELETE FROM video_milestones WHERE guild_id=? AND video_id=?", (guild_id, vid_id))
        await safe_send(interaction, f"✅ Removed `{vid_id}`")
    except Exception as e:
        logger.error(f"removevideo error: {e}")
        await safe_send(interaction, "❌ Remove failed", ephemeral=True)

@bot.tree.command(name="listvideos", description="List server videos")
async def listvideos(interaction: discord.Interaction):
    try:
        await interaction.response.defer()
        guild_id = str(interaction.guild.id)
        rows = await safe_db("SELECT video_id, title FROM guild_videos WHERE guild_id=?", (guild_id,), fetch=True) or []
        
        if not rows:
            return await safe_send(interaction, "📭 No videos on this server")
        
        embed = discord.Embed(title="📹 Server Videos", color=0x00ff00)
        for vid_id, title in rows[:10]:
            embed.add_field(name=title[:50], value=f"`{vid_id}`", inline=True)
        await safe_send(interaction, embed=embed)
    except Exception as e:
        logger.error(f"listvideos error: {e}")
        await safe_send(interaction, "❌ List failed", ephemeral=True)

@bot.tree.command(name="serverlist", description="All bot servers")
async def serverlist(interaction: discord.Interaction):
    try:
        await interaction.response.defer(ephemeral=True)
        embed = discord.Embed(title="🌐 All Servers", color=0x0099ff)
        for i, guild in enumerate(bot.guilds[:10], 1):
            embed.add_field(name=guild.name, value=f"{len(guild.members)} members", inline=True)
        embed.set_footer(text=f"Total: {len(bot.guilds)} servers")
        await safe_send(interaction, embed=embed)
    except Exception as e:
        logger.error(f"serverlist error: {e}")
        await safe_send(interaction, "❌ Server list failed", ephemeral=True)

@bot.tree.command(name="views", description="Single video stats")
@app_commands.describe(video="YouTube URL/ID")
async def views(interaction: discord.Interaction, video: str):
    try:
        await interaction.response.defer()
        vid_id = safe_extract_video_id(video)
        if not vid_id:
            return await safe_send(interaction, "❌ Invalid video", ephemeral=True)
        
        views, likes, title = await safe_yt_stats(vid_id)
        if views is None:
            return await safe_send(interaction, "❌ Video fetch failed")
        
        embed = discord.Embed(title="📊 Video Stats", color=0x00ff00)
        embed.add_field(name=title or vid_id, value=f"👀 **{views:,}** | ❤️ **{likes:,}**", inline=False)
        embed.url = f"https://youtube.com/watch?v={vid_id}"
        await safe_send(interaction, embed=embed)
    except Exception as e:
        logger.error(f"views error: {e}")
        await safe_send(interaction, "❌ Views failed", ephemeral=True)

# main.py - PART 3/5: Commands 7-12 (Checks + Milestones)
@bot.tree.command(name="viewsall", description="All server video stats")
async def viewsall(interaction: discord.Interaction):
    try:
        await interaction.response.defer()
        guild_id = str(interaction.guild.id)
        rows = await safe_db("SELECT video_id, title FROM guild_videos WHERE guild_id=?", (guild_id,), fetch=True) or []
        
        if not rows:
            return await safe_send(interaction, "📭 No server videos")
        
        embed = discord.Embed(title="📊 Server Stats", color=0x00ff00)
        for vid_id, title in rows[:10]:
            views, likes, _ = await safe_yt_stats(vid_id)
            views_str = f"{views:,}" if views else "N/A"
            embed.add_field(name=title[:40], value=f"`{vid_id[:10]}...`: {views_str}", inline=True)
        if len(rows) > 10:
            embed.set_footer(text=f"Showing 10/{len(rows)} videos")
        await safe_send(interaction, embed=embed)
    except Exception as e:
        logger.error(f"viewsall error: {e}")
        await safe_send(interaction, "❌ Views all failed", ephemeral=True)

@bot.tree.command(name="forcecheck", description="Force check channel videos")
async def forcecheck(interaction: discord.Interaction):
    try:
        await interaction.response.defer()
        guild_id = str(interaction.guild.id)
        channel_id = str(interaction.channel.id)
        rows = await safe_db("""
            SELECT video_id, title FROM guild_videos 
            WHERE guild_id=? AND channel_id=?
        """, (guild_id, channel_id), fetch=True) or []
        
        if not rows:
            return await safe_send(interaction, "📭 No videos in this channel")
        
        checked = 0
        for vid_id, title in rows:
            views, likes, _ = await safe_yt_stats(vid_id)
            if views:
                checked += 1
                channel = interaction.guild.get_channel(int(channel_id))
                if channel:
                    embed = discord.Embed(title="📊 Update", color=0x00ff00)
                    embed.add_field(name=title, value=f"**{views:,}** | ❤️ **{likes:,}**\n📺 https://youtube.com/watch?v={vid_id}", inline=False)
                    await channel.send(embed=embed)
        
        await safe_send(interaction, f"✅ Checked **{checked}/{len(rows)}** videos")
    except Exception as e:
        logger.error(f"forcecheck error: {e}")
        await safe_send(interaction, "❌ Force check failed", ephemeral=True)

@bot.tree.command(name="checkintervals", description="Check interval videos")
async def checkintervals(interaction: discord.Interaction):
    try:
        await interaction.response.defer()
        guild_id = str(interaction.guild.id)
        channel_id = str(interaction.channel.id)
        rows = await safe_db("""
            SELECT gv.video_id, gv.title FROM guild_videos gv 
            JOIN video_intervals vi ON gv.guild_id=vi.guild_id AND gv.video_id=vi.video_id
            WHERE gv.guild_id=? AND gv.channel_id=?
        """, (guild_id, channel_id), fetch=True) or []
        
        if not rows:
            return await safe_send(interaction, "📭 No interval videos in channel")
        
        checked = 0
        for vid_id, title in rows:
            views, likes, _ = await safe_yt_stats(vid_id)
            if views:
                checked += 1
        
        await safe_send(interaction, f"✅ Checked **{checked}/{len(rows)}** interval videos")
    except Exception as e:
        logger.error(f"checkintervals error: {e}")
        await safe_send(interaction, "❌ Interval check failed", ephemeral=True)

@bot.tree.command(name="reachedmilestones", description="Set/reached milestones")
async def reachedmilestones(interaction: discord.Interaction):
    try:
        await interaction.response.defer()
        guild_id = str(interaction.guild.id)
        rows = await safe_db("""
            SELECT video_id, title, target FROM video_milestones 
            WHERE guild_id=?
        """, (guild_id,), fetch=True) or []
        
        if not rows:
            return await safe_send(interaction, "📭 No milestones set")
        
        embed = discord.Embed(title="🎉 Milestones", color=0xffd700)
        for vid_id, title, target in rows[:10]:
            embed.add_field(name=title[:50], value=f"**{target:,}** views\n`{vid_id}`", inline=True)
        await safe_send(interaction, embed=embed)
    except Exception as e:
        logger.error(f"reachedmilestones error: {e}")
        await safe_send(interaction, "❌ Milestones failed", ephemeral=True)

@bot.tree.command(name="upcoming", description="Upcoming milestones <100K")
async def upcoming(interaction: discord.Interaction):
    try:
        await interaction.response.defer()
        guild_id = str(interaction.guild.id)
        rows = await safe_db("""
            SELECT gv.video_id, gv.title, vm.target 
            FROM guild_videos gv JOIN video_milestones vm ON gv.guild_id=vm.guild_id AND gv.video_id=vm.video_id
            WHERE gv.guild_id=?
        """, (guild_id,), fetch=True) or []
        
        embed = discord.Embed(title="⏳ Upcoming (<100K)", color=0x0099ff)
        has_upcoming = False
        for vid_id, title, target in rows:
            views, _, _ = await safe_yt_stats(vid_id)
            if views and target and (target - views) <= 100000 and (target - views) > 0:
                remaining = target - views
                embed.add_field(name=title[:50], value=f"{remaining:,} to **{target:,}**", inline=True)
                has_upcoming = True
        
        if has_upcoming:
            await safe_send(interaction, embed=embed)
        else:
            await safe_send(interaction, "📭 No videos within 100K of milestones")
    except Exception as e:
        logger.error(f"upcoming error: {e}")
        await safe_send(interaction, "❌ Upcoming failed", ephemeral=True)

@bot.tree.command(name="setmilestone", description="Set custom milestone")
@app_commands.describe(video="YouTube URL/ID", target="Target views", channel="Alert channel")
async def setmilestone(interaction: discord.Interaction, video: str, target: int, 
                      channel: discord.TextChannel = None, ping: str = ""):
    try:
        await interaction.response.defer()
        vid_id = safe_extract_video_id(video)
        if not vid_id:
            return await safe_send(interaction, "❌ Invalid video", ephemeral=True)
        
        if target < 1000:
            return await safe_send(interaction, "❌ Target must be ≥ 1,000", ephemeral=True)
        
        guild_id = str(interaction.guild.id)
        alert_ch = str(channel.id if channel else interaction.channel.id)
        
        await safe_db("""
            INSERT OR REPLACE INTO video_milestones 
            (guild_id, video_id, target, alert_channel, ping)
            VALUES (?, ?, ?, ?, ?)
        """, (guild_id, vid_id, target, alert_ch, ping))
        
        await safe_send(interaction, f"✅ **{target:,}** milestone → {channel.mention if channel else interaction.channel.mention} {ping}")
    except Exception as e:
        logger.error(f"setmilestone error: {e}")
        await safe_send(interaction, "❌ Milestone failed", ephemeral=True)

# main.py - PART 4/5: Commands 13-19 (Intervals + Status)
@bot.tree.command(name="removemilestones", description="Remove video milestones")
@app_commands.describe(video="YouTube URL/ID")
async def removemilestones(interaction: discord.Interaction, video: str):
    try:
        await interaction.response.defer()
        vid_id = safe_extract_video_id(video)
        if not vid_id:
            return await safe_send(interaction, "❌ Invalid video", ephemeral=True)
        
        guild_id = str(interaction.guild.id)
        await safe_db("DELETE FROM video_milestones WHERE guild_id=? AND video_id=?", (guild_id, vid_id))
        await safe_send(interaction, f"✅ Cleared milestones for `{vid_id}`")
    except Exception as e:
        logger.error(f"removemilestones error: {e}")
        await safe_send(interaction, "❌ Remove failed", ephemeral=True)

@bot.tree.command(name="setinterval", description="Set video check interval")
@app_commands.describe(video="YouTube URL/ID", minutes="1-1440 minutes")
async def setinterval(interaction: discord.Interaction, video: str, minutes: int):
    try:
        await interaction.response.defer()
        if not (1 <= minutes <= 1440):
            return await safe_send(interaction, "❌ Must be 1-1440 minutes (24h)", ephemeral=True)
        
        vid_id = safe_extract_video_id(video)
        if not vid_id:
            return await safe_send(interaction, "❌ Invalid video", ephemeral=True)
        
        guild_id = str(interaction.guild.id)
        next_check = (datetime.now(KST) + timedelta(minutes=minutes)).isoformat()
        
        await safe_db("""
            INSERT OR REPLACE INTO video_intervals 
            (guild_id, video_id, minutes, next_check)
            VALUES (?, ?, ?, ?)
        """, (guild_id, vid_id, minutes, next_check))
        
        await safe_send(interaction, f"✅ **{minutes}min** interval set for `{vid_id}`")
    except Exception as e:
        logger.error(f"setinterval error: {e}")
        await safe_send(interaction, "❌ Interval failed", ephemeral=True)

@bot.tree.command(name="disableinterval", description="Stop video interval")
@app_commands.describe(video="YouTube URL/ID")
async def disableinterval(interaction: discord.Interaction, video: str):
    try:
        await interaction.response.defer()
        vid_id = safe_extract_video_id(video)
        if not vid_id:
            return await safe_send(interaction, "❌ Invalid video", ephemeral=True)
        
        guild_id = str(interaction.guild.id)
        await safe_db("DELETE FROM video_intervals WHERE guild_id=? AND video_id=?", (guild_id, vid_id))
        await safe_send(interaction, f"✅ Disabled interval for `{vid_id}`")
    except Exception as e:
        logger.error(f"disableinterval error: {e}")
        await safe_send(interaction, "❌ Disable failed", ephemeral=True)

@bot.tree.command(name="listintervals", description="List server intervals")
async def listintervals(interaction: discord.Interaction):
    try:
        await interaction.response.defer()
        guild_id = str(interaction.guild.id)
        rows = await safe_db("""
            SELECT video_id, minutes FROM video_intervals 
            WHERE guild_id=?
        """, (guild_id,), fetch=True) or []
        
        if not rows:
            return await safe_send(interaction, "📭 No intervals set")
        
        embed = discord.Embed(title="⏱️ Intervals", color=0x0099ff)
        for vid_id, minutes in rows[:10]:
            embed.add_field(name=f"`{vid_id}`", value=f"{minutes}min", inline=True)
        await safe_send(interaction, embed=embed)
    except Exception as e:
        logger.error(f"listintervals error: {e}")
        await safe_send(interaction, "❌ List failed", ephemeral=True)

@bot.tree.command(name="setupcomingmilestonesalert", description="Setup upcoming alerts")
@app_commands.describe(channel="Alert channel", ping="Optional ping")
async def setupcomingmilestonesalert(interaction: discord.Interaction, 
                                   channel: discord.TextChannel = None, ping: str = ""):
    try:
        await interaction.response.defer()
        guild_id = str(interaction.guild.id)
        alert_ch = str(channel.id if channel else interaction.channel.id)
        
        await safe_db("""
            INSERT OR REPLACE INTO guild_settings 
            (guild_id, upcoming_channel, upcoming_ping)
            VALUES (?, ?, ?)
        """, (guild_id, alert_ch, ping))
        
        await safe_send(interaction, f"✅ Upcoming alerts → {channel.mention if channel else interaction.channel.mention} {ping}")
    except Exception as e:
        logger.error(f"setupcoming error: {e}")
        await safe_send(interaction, "❌ Setup failed", ephemeral=True)

@bot.tree.command(name="servercheck", description="Server statistics")
async def servercheck(interaction: discord.Interaction):
    try:
        await interaction.response.defer()
        guild_id = str(interaction.guild.id)
        
        videos = len(await safe_db("SELECT 1 FROM guild_videos WHERE guild_id=?", (guild_id,), fetch=True) or [])
        intervals = len(await safe_db("SELECT 1 FROM video_intervals WHERE guild_id=?", (guild_id,), fetch=True) or [])
        
        embed = discord.Embed(title="📊 Server Stats", color=0x00ff00)
        embed.add_field(name="Videos", value=videos, inline=True)
        embed.add_field(name="Intervals", value=intervals, inline=True)
        embed.add_field(name="Members", value=len(interaction.guild.members), inline=True)
        await safe_send(interaction, embed=embed)
    except Exception as e:
        logger.error(f"servercheck error: {e}")
        await safe_send(interaction, "❌ Server check failed", ephemeral=True)

# main.py - PART 5/5: Help + Tasks + Startup (FINAL)
@bot.tree.command(name="help", description="All 19 commands")
async def help_cmd(interaction: discord.Interaction):
    try:
        embed = discord.Embed(title="📋 19 YouTube Tracker Commands", color=0x0099ff)
        embed.add_field(
            name="📹 **Video Management**", 
            value="`/addvideo` `/removevideo` `/listvideos` `/views`", 
            inline=False
        )
        embed.add_field(
            name="🔄 **Live Checks**", 
            value="`/forcecheck` `/viewsall` `/checkintervals`", 
            inline=False
        )
        embed.add_field(
            name="⏱️ **Custom Intervals**", 
            value="`/setinterval` `/disableinterval` `/listintervals`", 
            inline=False
        )
        embed.add_field(
            name="🎯 **Milestones**", 
            value="`/setmilestone` `/removemilestones` `/upcoming`", 
            inline=False
        )
        embed.add_field(
            name="📊 **Status**", 
            value="`/botcheck` `/servercheck` `/serverlist`", 
            inline=False
        )
        embed.set_footer(text="KST: 12AM/12PM/5PM + Custom Intervals | Render Ready")
        await safe_send(interaction, embed=embed, ephemeral=True)
    except Exception as e:
        logger.error(f"help error: {e}")
        await safe_send(interaction, "❌ Help failed", ephemeral=True)

# BACKGROUND TASKS
@tasks.loop(minutes=1)
async def kst_checker():
    """KST: 12AM/12PM/5PM precise checks."""
    try:
        now = datetime.now(KST)
        if now.hour in [0, 12, 17] and now.minute == 0:
            logger.info(f"🕐 KST {now.hour}:00 Check running")
            guilds = await safe_db("SELECT DISTINCT guild_id FROM guild_videos", fetch=True) or []
            for guild_id_tuple in guilds:
                guild_id = guild_id_tuple[0]
                guild = bot.get_guild(int(guild_id))
                if guild:
                    rows = await safe_db(
                        "SELECT video_id, title, channel_id FROM guild_videos WHERE guild_id=?", 
                        (guild_id,), fetch=True
                    ) or []
                    for vid_id, title, ch_id in rows:
                        views, likes, _ = await safe_yt_stats(vid_id)
                        if views:
                            channel = guild.get_channel(int(ch_id))
                            if channel:
                                embed = discord.Embed(title="📊 KST Update", color=0x00ff00)
                                embed.add_field(
                                    name=title[:100], 
                                    value=f"**{views:,} views** | ❤️ **{likes:,}**\n📺 https://youtube.com/watch?v={vid_id}",
                                    inline=False
                                )
                                await channel.send(embed=embed)
    except Exception as e:
        logger.error(f"KST checker error: {e}")

@tasks.loop(minutes=1)
async def interval_checker():
    """Custom interval checks."""
    try:
        now_str = datetime.now(KST).isoformat()
        rows = await safe_db("""
            SELECT guild_id, video_id, channel_id, minutes 
            FROM video_intervals vi 
            JOIN guild_videos gv ON vi.guild_id=gv.guild_id AND vi.video_id=gv.video_id
            WHERE datetime(vi.next_check) <= ?
        """, (now_str,), fetch=True) or []
        
        for guild_id, vid_id, ch_id, minutes in rows:
            views, likes, title = await safe_yt_stats(vid_id)
            if views:
                guild = bot.get_guild(int(guild_id))
                channel = guild.get_channel(int(ch_id)) if guild else None
                if channel:
                    embed = discord.Embed(title="⏱️ Interval Update", color=0x0099ff)
                    embed.add_field(
                        name=title or vid_id,
                        value=f"**{views:,} views** | ❤️ **{likes:,}**\n📺 https://youtube.com/watch?v={vid_id}",
                        inline=False
                    )
                    await channel.send(embed=embed)
                
                # Reschedule next check
                next_check = (datetime.now(KST) + timedelta(minutes=minutes)).isoformat()
                await safe_db(
                    "UPDATE video_intervals SET next_check=? WHERE guild_id=? AND video_id=?",
                    (next_check, guild_id, vid_id)
                )
    except Exception as e:
        logger.error(f"Interval checker error: {e}")

# STARTUP
@bot.event
async def on_ready():
    try:
        await init_db()
        logger.info("✅ Database initialized")

        synced = await bot.tree.sync()
        logger.info(f"✅ Synced {len(synced)} slash commands")

        # Start tasks (CORRECT INDENT + SYNTAX)
        kst_checker.before_loop(bot.wait_until_ready())
        interval_checker.before_loop(bot.wait_until_ready())
        kst_checker.start()
        interval_checker.start()

        # Flask keepalive
        threading.Thread(target=run_flask, daemon=True).start()
        logger.info(f"🌐 Flask started on port {PORT}")

        logger.info(f"🚀 {bot.user} ready! 19 commands + KST + Intervals ACTIVE!")
        logger.info(f"📱 Servers: {len(bot.guilds)} | DB: {DB_PATH}")
    except Exception as e:
        logger.error(f"Startup error: {e}")
