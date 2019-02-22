from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

from discord.ext import commands
import discord
import logging
import os
import sys

sys.path.insert(0, 'src/')
import dbase as db
import utility


logging.basicConfig(
    level=logging.INFO,
    style = '{',
    datefmt = "%Y%m%d %H:%M:%S",
    format = "{asctime} [{levelname:<8}] {name}: {message}")

if 'BOT_PREFIX' in os.environ:
    BOT_PREFIX = str(os.environ.get('BOT_PREFIX'))
else:
    BOT_PREFIX=','

bot = commands.Bot(command_prefix=BOT_PREFIX,
                   description='Heroku Discord Bot Example')
setattr(bot, "logger", logging.getLogger("bot.py"))

print('Connecting to database')
HD = db.HerokuDiscordTable()


# ----- Bot Events ------------------------------------------------------------
@bot.event
async def on_ready():
    print(f'Bot logged in as {bot.user.name} (id={bot.user.id})')
    print(f'Bot prefix is {BOT_PREFIX}')


@bot.event
async def on_command_error(ctx, err):
    if isinstance(err, commands.CommandOnCooldown):
        await ctx.send('Error: {}'.format(err))


# ----- Bot Commands ----------------------------------------------------------
@bot.command(brief='Ping the server')
async def ping(ctx):
    """Ping the server to verify that it\'s listening for commands"""
    await ctx.send('Pong!')

    
@bot.command(brief='Run shell command')
@commands.is_owner()
async def shell(ctx, *, cmd):
    """Run a shell command"""
    txt = utility.shell_cmd(cmd)
    await ctx.send(txt)


@bot.command(brief='About this bot')
async def about(ctx):
    """Gives a description for this bot"""
    txt = (
        "This bot serves as a template for a Discord bot"
        "that can be hosted on Heroku.\n"
        "The Github page for this bot is: "
        "<https://github.com/jzx3/heroku-discord-template>")
    await ctx.send(txt)


# ----- Bot Database Commands (Admin) -----------------------------------------
@bot.command(brief='Check table')
@commands.is_owner()
async def db_check(ctx):
    """Check the connection to the database"""
    txt = HD.check_connection()
    await ctx.send(txt)


@bot.command(brief='Create table')
@commands.is_owner()
async def db_create(ctx):
    """Create the initial table for the database"""
    txt = HD.create()
    await ctx.send(txt)


@bot.command(brief='Delete table')
@commands.is_owner()
async def db_delete(ctx):
    """Deletes the table for the database"""
    txt = HD.drop()
    await ctx.send(txt)


@bot.command(brief='Add column')
@commands.is_owner()
async def db_add_column(ctx, column_name, data_type):
    """Add a column to the table"""
    txt = HD.add_column(column_name, data_type)
    await ctx.send(txt)


@bot.command(brief='Delete column')
@commands.is_owner()
async def db_rm_column(ctx, column_name):
    """Removes a column from the table"""
    txt = HD.drop_column(column_name)
    await ctx.send(txt)


@bot.command(brief='Read table')
@commands.is_owner()
async def db_read(ctx):
    """Read the table"""
    txt = HD.read()
    await ctx.send(txt)


@bot.command(brief='Run sql query')
@commands.is_owner()
async def sql_commit(ctx, *, sql_query):
    """Run a custom sql query + commit"""
    txt = HD.sql_commit(sql_query)
    await ctx.send(txt)


@bot.command(brief='Run sql fetch')
@commands.is_owner()
async def sql_fetch(ctx, *, sql_query):
    """Run a custom sql query + fetch"""
    txt = HD.sql_fetch(sql_query)
    await ctx.send(txt)


# ----- Bot Database Commands (User) ------------------------------------------
@bot.command(brief='Read database entry')
async def db_getrow(ctx):
    """Read a row of the table corresponding to your Discord ID"""
    txt = HD.getrow(ctx.author.id)
    await ctx.send(txt)


@bot.command(brief='Set your home Discord')
async def home_discord(ctx):
    """Sets your home discord"""
    txt = HD.set_home_discord(ctx.author.id, ctx.server.id, ctx.author.name, ctx.server.id)
    await ctx.send(txt)


@bot.command(brief='Set your in-game name')
async def ign(ctx, *, ign):
    """Set your in-game name"""
    txt = HD.insert_ign(ctx.author.id, ctx.server.id, ctx.author.name, ign)
    await ctx.send(txt)


@bot.command(brief='Insert local data')
async def db_insert_local(ctx, *, txt):
    """Insert data into the table (restricted to your current Discord)"""
    txt = HD.insert_local(ctx.author.id, ctx.server.id, ctx.author.name, txt)
    await ctx.send(txt)


@bot.command(brief='Insert global data')
async def db_insert_global(ctx, *, txt):
    """Insert data into the table (shared across all Discords)"""
    txt = HD.insert_local(ctx.author.id, ctx.server.id, ctx.author.name, txt)
    await ctx.send(txt)


# ----- Run the Bot -----------------------------------------------------------
if __name__ == '__main__':
    token = str(os.environ.get('DISCORD_BOT_TOKEN'))
    bot.run(token)
    HD.close()
