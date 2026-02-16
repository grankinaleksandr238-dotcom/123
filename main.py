import asyncio
import logging
import random
import os
import time
from datetime import datetime, timedelta
from aiogram import Bot, Dispatcher, types
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters.state import State, StatesGroup
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.utils import executor
from aiogram.utils.exceptions import (
    BotBlocked, UserDeactivated, ChatNotFound, RetryAfter,
    TelegramAPIError, MessageNotModified, MessageToEditNotFound,
    TerminatedByOtherGetUpdates
)
import aiosqlite
from aiohttp import web

# ===== НАСТРОЙКИ =====
BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN не задан в переменных окружения")

SUPER_ADMINS_STR = os.getenv("SUPER_ADMINS", "")
SUPER_ADMINS = [int(x.strip()) for x in SUPER_ADMINS_STR.split(",") if x.strip()]

# Для постоянного хранения данных используй Volume на Railway с путём /data
DB_PATH = os.getenv("DB_PATH", "database.db")  # если не задано, будет локальный файл

# Значения по умолчанию для настроек
DEFAULT_SETTINGS = {
    "random_attack_cost": "0",
    "targeted_attack_cost": "50",
    "theft_cooldown_minutes": "30",
    "theft_success_chance": "40",
    "theft_defense_chance": "20",
    "theft_defense_penalty": "10",
    "casino_win_chance": "30",
    "min_theft_amount": "5",
    "max_theft_amount": "15"
}

# ===== ИНИЦИАЛИЗАЦИЯ =====
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s"
)

# Принудительно удаляем вебхук (если был) и пропускаем старые обновления
async def before_start():
    await bot.delete_webhook(drop_pending_updates=True)
    logging.info("Webhook удалён, пропущены старые обновления")

bot = Bot(token=BOT_TOKEN, parse_mode="HTML")
storage = MemoryStorage()
dp = Dispatcher(bot, storage=storage)

# ===== БЕЗОПАСНАЯ ОТПРАВКА СООБЩЕНИЙ =====
async def safe_send_message(user_id: int, text: str, **kwargs):
    try:
        await bot.send_message(user_id, text, **kwargs)
    except BotBlocked:
        logging.warning(f"Bot blocked by user {user_id}")
    except UserDeactivated:
        logging.warning(f"User {user_id} deactivated")
    except ChatNotFound:
        logging.warning(f"Chat {user_id} not found")
    except RetryAfter as e:
        logging.warning(f"Flood limit exceeded. Retry after {e.timeout} seconds")
        await asyncio.sleep(e.timeout)
        try:
            await bot.send_message(user_id, text, **kwargs)
        except Exception as ex:
            logging.warning(f"Still failed after retry: {ex}")
    except TelegramAPIError as e:
        logging.warning(f"Telegram API error for user {user_id}: {e}")
    except Exception as e:
        logging.warning(f"Failed to send message to {user_id}: {e}")

def safe_send_message_task(user_id: int, text: str, **kwargs):
    asyncio.create_task(safe_send_message(user_id, text, **kwargs))

# ===== БАЗА ДАННЫХ =====
async def init_db():
    # Создаём директорию для базы, если её нет (нужно для Volume)
    db_dir = os.path.dirname(DB_PATH)
    if db_dir and not os.path.exists(db_dir):
        os.makedirs(db_dir, exist_ok=True)

    async with aiosqlite.connect(DB_PATH, timeout=10) as db:
        await db.execute("PRAGMA journal_mode=WAL")
        await db.execute("PRAGMA busy_timeout=5000")
        
        # Пользователи
        await db.execute('''
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                joined_date TEXT,
                balance INTEGER DEFAULT 0,
                last_bonus TEXT,
                last_theft_time TEXT,
                theft_attempts INTEGER DEFAULT 0,
                theft_success INTEGER DEFAULT 0,
                theft_failed INTEGER DEFAULT 0,
                theft_protected INTEGER DEFAULT 0
            )
        ''')

        # Каналы для подписки
        await db.execute('''
            CREATE TABLE IF NOT EXISTS channels (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                chat_id TEXT UNIQUE,
                title TEXT,
                invite_link TEXT
            )
        ''')

        # Товары магазина (только подарки)
        await db.execute('''
            CREATE TABLE IF NOT EXISTS shop_items (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT,
                description TEXT,
                price INTEGER,
                stock INTEGER DEFAULT -1
            )
        ''')

        # Покупки подарков
        await db.execute('''
            CREATE TABLE IF NOT EXISTS purchases (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                item_id INTEGER,
                purchase_date TEXT,
                status TEXT DEFAULT 'pending',
                admin_comment TEXT
            )
        ''')

        # Промокоды
        await db.execute('''
            CREATE TABLE IF NOT EXISTS promocodes (
                code TEXT PRIMARY KEY,
                reward INTEGER,
                max_uses INTEGER,
                used_count INTEGER DEFAULT 0
            )
        ''')

        # Розыгрыши
        await db.execute('''
            CREATE TABLE IF NOT EXISTS giveaways (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                prize TEXT,
                description TEXT,
                end_date TEXT,
                media_file_id TEXT,
                media_type TEXT,
                status TEXT DEFAULT 'active',
                winner_id INTEGER,
                winners_count INTEGER DEFAULT 1
            )
        ''')

        # Участники розыгрышей
        await db.execute('''
            CREATE TABLE IF NOT EXISTS participants (
                user_id INTEGER,
                giveaway_id INTEGER,
                FOREIGN KEY(user_id) REFERENCES users(user_id),
                FOREIGN KEY(giveaway_id) REFERENCES giveaways(id),
                PRIMARY KEY (user_id, giveaway_id)
            )
        ''')

        # Младшие админы
        await db.execute('''
            CREATE TABLE IF NOT EXISTS admins (
                user_id INTEGER PRIMARY KEY,
                added_by INTEGER,
                added_date TEXT
            )
        ''')

        # Заблокированные пользователи
        await db.execute('''
            CREATE TABLE IF NOT EXISTS banned_users (
                user_id INTEGER PRIMARY KEY,
                banned_by INTEGER,
                banned_date TEXT,
                reason TEXT
            )
        ''')

        # Настройки игры
        await db.execute('''
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT
            )
        ''')

        # Индексы
        await db.execute("CREATE INDEX IF NOT EXISTS idx_users_username ON users(username)")
        await db.commit()

    await create_default_items()
    await init_settings()

async def create_default_items():
    default_items = [
        ("🎁 Цветы", "Красивый букет", 50, 10),
        ("🎁 Конфеты", "Коробка шоколадных конфет", 30, 10),
        ("🎁 Игрушка", "Мягкая игрушка", 70, 5),
    ]
    async with aiosqlite.connect(DB_PATH, timeout=10) as db:
        for name, desc, price, stock in default_items:
            cur = await db.execute("SELECT id FROM shop_items WHERE name=?", (name,))
            if not await cur.fetchone():
                await db.execute(
                    "INSERT INTO shop_items (name, description, price, stock) VALUES (?, ?, ?, ?)",
                    (name, desc, price, stock)
                )
        await db.commit()

async def init_settings():
    async with aiosqlite.connect(DB_PATH, timeout=10) as db:
        for key, value in DEFAULT_SETTINGS.items():
            await db.execute("INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)", (key, value))
        await db.commit()

async def get_setting(key: str) -> str:
    async with aiosqlite.connect(DB_PATH, timeout=10) as db:
        async with db.execute("SELECT value FROM settings WHERE key=?", (key,)) as cur:
            row = await cur.fetchone()
            return row[0] if row else DEFAULT_SETTINGS[key]

async def set_setting(key: str, value: str):
    async with aiosqlite.connect(DB_PATH, timeout=10) as db:
        await db.execute("UPDATE settings SET value=? WHERE key=?", (value, key))
        await db.commit()

# ===== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ =====
async def is_super_admin(user_id: int) -> bool:
    return user_id in SUPER_ADMINS

async def is_junior_admin(user_id: int) -> bool:
    async with aiosqlite.connect(DB_PATH, timeout=10) as db:
        async with db.execute("SELECT user_id FROM admins WHERE user_id=?", (user_id,)) as cur:
            row = await cur.fetchone()
    return row is not None

async def is_admin(user_id: int) -> bool:
    return await is_super_admin(user_id) or await is_junior_admin(user_id)

async def is_banned(user_id: int) -> bool:
    async with aiosqlite.connect(DB_PATH, timeout=10) as db:
        async with db.execute("SELECT user_id FROM banned_users WHERE user_id=?", (user_id,)) as cur:
            row = await cur.fetchone()
    return row is not None

async def get_channels():
    async with aiosqlite.connect(DB_PATH, timeout=10) as db:
        async with db.execute("SELECT chat_id, title, invite_link FROM channels") as cursor:
            return await cursor.fetchall()

async def check_subscription(user_id: int):
    channels = await get_channels()
    if not channels:
        return True, []
    not_subscribed = []
    for chat_id, title, link in channels:
        try:
            member = await bot.get_chat_member(chat_id=chat_id, user_id=user_id)
            if member.status in ['left', 'kicked']:
                not_subscribed.append((title, link))
        except Exception:
            not_subscribed.append((title, link))
    return len(not_subscribed) == 0, not_subscribed

async def get_user_balance(user_id: int) -> int:
    async with aiosqlite.connect(DB_PATH, timeout=10) as db:
        async with db.execute("SELECT balance FROM users WHERE user_id=?", (user_id,)) as cur:
            row = await cur.fetchone()
            return row[0] if row else 0

async def update_user_balance(user_id: int, delta: int):
    async with aiosqlite.connect(DB_PATH, timeout=10) as db:
        await db.execute("UPDATE users SET balance = balance + ? WHERE user_id=?", (delta, user_id))
        await db.commit()

async def get_random_user(exclude_id: int):
    async with aiosqlite.connect(DB_PATH, timeout=10) as db:
        async with db.execute("""
            SELECT user_id FROM users 
            WHERE user_id != ? AND user_id NOT IN (SELECT user_id FROM banned_users)
            ORDER BY RANDOM() LIMIT 1
        """, (exclude_id,)) as cur:
            row = await cur.fetchone()
            return row[0] if row else None

# ===== СОСТОЯНИЯ FSM =====
class CreateGiveaway(StatesGroup):
    prize = State()
    description = State()
    end_date = State()
    media = State()

class AddChannel(StatesGroup):
    chat_id = State()
    title = State()
    invite_link = State()

class RemoveChannel(StatesGroup):
    chat_id = State()

class AddShopItem(StatesGroup):
    name = State()
    description = State()
    price = State()
    stock = State()

class RemoveShopItem(StatesGroup):
    item_id = State()

class EditShopItem(StatesGroup):
    item_id = State()
    field = State()
    value = State()

class CreatePromocode(StatesGroup):
    code = State()
    reward = State()
    max_uses = State()

class Broadcast(StatesGroup):
    media = State()

class AddBalance(StatesGroup):
    user_id = State()
    amount = State()

class RemoveBalance(StatesGroup):
    user_id = State()
    amount = State()

class CasinoBet(StatesGroup):
    amount = State()

class PromoActivate(StatesGroup):
    code = State()

class TheftTarget(StatesGroup):
    target = State()

class FindUser(StatesGroup):
    query = State()

class AddJuniorAdmin(StatesGroup):
    user_id = State()

class RemoveJuniorAdmin(StatesGroup):
    user_id = State()

class CompleteGiveaway(StatesGroup):
    giveaway_id = State()
    winners_count = State()

class BlockUser(StatesGroup):
    user_id = State()
    reason = State()

class UnblockUser(StatesGroup):
    user_id = State()

class EditSettings(StatesGroup):
    key = State()
    value = State()

# ===== КЛАВИАТУРЫ =====
def subscription_inline(not_subscribed):
    kb = []
    for title, link in not_subscribed:
        if link:
            kb.append([InlineKeyboardButton(text=f"📢 {title}", url=link)])
        else:
            kb.append([InlineKeyboardButton(text=f"📢 {title}", callback_data="no_link")])
    kb.append([InlineKeyboardButton(text="✅ Я подписался", callback_data="check_sub")])
    return InlineKeyboardMarkup(row_width=1, inline_keyboard=kb)

def user_main_keyboard(is_admin_user=False):
    buttons = [
        [KeyboardButton(text="👤 Профиль"), KeyboardButton(text="🎁 Бонус")],
        [KeyboardButton(text="🛒 Магазин подарков"), KeyboardButton(text="🎰 Казино")],
        [KeyboardButton(text="🎟 Промокод"), KeyboardButton(text="🏆 Топ игроков")],
        [KeyboardButton(text="💰 Мои покупки"), KeyboardButton(text="🔫 Ограбить")],
    ]
    if is_admin_user:
        buttons.append([KeyboardButton(text="⚙️ Админ панель")])
    return ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True)

def theft_choice_keyboard():
    return ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="🎲 Случайная цель")],
        [KeyboardButton(text="👤 Выбрать пользователя")],
        [KeyboardButton(text="◀️ Назад")]
    ], resize_keyboard=True)

def admin_main_keyboard(is_super):
    buttons = [
        [KeyboardButton(text="🎁 Управление розыгрышами")],
        [KeyboardButton(text="📢 Рассылка"), KeyboardButton(text="💰 Начислить монеты")],
        [KeyboardButton(text="📺 Управление каналами")],
        [KeyboardButton(text="🛒 Управление магазином")],
        [KeyboardButton(text="🎫 Управление промокодами")],
        [KeyboardButton(text="⚙️ Настройки игры")],
        [KeyboardButton(text="📊 Статистика")],
        [KeyboardButton(text="👥 Найти пользователя")],
        [KeyboardButton(text="🛍️ Список покупок")],
        [KeyboardButton(text="🔨 Заблокировать пользователя")],
        [KeyboardButton(text="🔓 Разблокировать пользователя")],
        [KeyboardButton(text="💸 Списать монеты")],
    ]
    if is_super:
        buttons.append([KeyboardButton(text="➕ Добавить админа")])
        buttons.append([KeyboardButton(text="➖ Удалить админа")])
        buttons.append([KeyboardButton(text="🔄 Сброс статистики")])
    buttons.append([KeyboardButton(text="◀️ Назад в главное меню")])
    return ReplyKeyboardMarkup(keyboard=buttons, resize_keyboard=True)

def settings_keyboard():
    return InlineKeyboardMarkup(row_width=2, inline_keyboard=[
        [InlineKeyboardButton(text="💰 Стоимость случайной кражи", callback_data="set_random_cost")],
        [InlineKeyboardButton(text="👤 Стоимость кражи по username", callback_data="set_targeted_cost")],
        [InlineKeyboardButton(text="⏱ Кулдаун (минут)", callback_data="set_cooldown")],
        [InlineKeyboardButton(text="🎲 Шанс успеха %", callback_data="set_success_chance")],
        [InlineKeyboardButton(text="🛡 Шанс защиты %", callback_data="set_defense_chance")],
        [InlineKeyboardButton(text="💥 Штраф при защите", callback_data="set_defense_penalty")],
        [InlineKeyboardButton(text="🎰 Шанс казино %", callback_data="set_casino_chance")],
        [InlineKeyboardButton(text="💰 Мин. сумма кражи", callback_data="set_min_theft")],
        [InlineKeyboardButton(text="💰 Макс. сумма кражи", callback_data="set_max_theft")],
        [InlineKeyboardButton(text="◀️ Назад", callback_data="back_to_admin")]
    ])

def giveaway_admin_keyboard():
    return ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="➕ Создать розыгрыш")],
        [KeyboardButton(text="📋 Активные розыгрыши")],
        [KeyboardButton(text="✅ Завершить розыгрыш")],
        [KeyboardButton(text="◀️ Назад в админку")]
    ], resize_keyboard=True)

def channel_admin_keyboard():
    return ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="➕ Добавить канал")],
        [KeyboardButton(text="➖ Удалить канал")],
        [KeyboardButton(text="📋 Список каналов")],
        [KeyboardButton(text="◀️ Назад в админку")]
    ], resize_keyboard=True)

def shop_admin_keyboard():
    return ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="➕ Добавить товар")],
        [KeyboardButton(text="➖ Удалить товар")],
        [KeyboardButton(text="✏️ Редактировать товар")],
        [KeyboardButton(text="📋 Список товаров")],
        [KeyboardButton(text="◀️ Назад в админку")]
    ], resize_keyboard=True)

def promo_admin_keyboard():
    return ReplyKeyboardMarkup(keyboard=[
        [KeyboardButton(text="➕ Создать промокод")],
        [KeyboardButton(text="📋 Список промокодов")],
        [KeyboardButton(text="◀️ Назад в админку")]
    ], resize_keyboard=True)

def back_keyboard():
    return ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="◀️ Назад")]], resize_keyboard=True)

def purchase_action_keyboard(purchase_id):
    return InlineKeyboardMarkup(row_width=2, inline_keyboard=[
        [InlineKeyboardButton(text="✅ Выполнено", callback_data=f"purchase_done_{purchase_id}"),
         InlineKeyboardButton(text="❌ Отказ", callback_data=f"purchase_reject_{purchase_id}")]
    ])

# ===== ТЕКСТОВЫЕ ФРАЗЫ =====
BONUS_PHRASES = [
    "🎉 Красава, лови +{bonus} монет!",
    "💰 Зашкварно богатенький стал! +{bonus}",
    "🌟 Хайпанули? +{bonus} монет в карман!",
    "🍀 Удача крашеная, держи +{bonus}",
    "🎁 Ты в тренде, +{bonus} монет!"
]

CASINO_WIN_PHRASES = [
    "🎰 Краш! Ты выиграл {win} монет (чистыми {profit})!",
    "🍒 Хайповая комбинация! +{profit} монет!",
    "💫 Фортуна крашеная, твой выигрыш: {win} монет!",
    "🎲 Изи-катка, {profit} монет твои!",
    "✨ Ты красавчик, обыграл казино! +{profit} монет!"
]

CASINO_LOSE_PHRASES = [
    "😢 Обидно, потерял {loss} монет.",
    "💔 Зашкварно, минус {loss}.",
    "📉 Не фортануло, -{loss} монет.",
    "🍂 В следующий раз краш будет твоим, а пока -{loss}.",
    "⚡️ Лузернулся на {loss} монет."
]

PURCHASE_PHRASES = [
    "✅ Купил! Админ скоро в личку прилетит.",
    "🛒 Товар твой! Жди админа, бро.",
    "🎁 Крутая покупка! Админ уже в курсе.",
    "💎 Ты краш! Админ свяжется."
]

THEFT_CHOICE_PHRASES = [
    "🔫 Выбери, как хочешь напасть:",
    "💢 Кого будем грабить?",
    "😈 Куда направим бандитские лапы?"
]

THEFT_COOLDOWN_PHRASES = [
    "⏳ Ты ещё не остыл после прошлого налёта. Подожди {minutes} мин.",
    "🕐 Полегче, ковбой! Отдохни {minutes} минут.",
    "😴 Грабить так часто – плохая примета. Возвращайся через {minutes} мин."
]

THEFT_NO_MONEY_PHRASES = [
    "😕 У тебя нет монет даже на подготовку к краже!",
    "💸 Сначала заработай, потом грабить будешь.",
    "💰 Пустой карман – не до криминала."
]

THEFT_SUCCESS_PHRASES = [
    "🔫 Красава! Ты украл {amount} монет у {target}!",
    "💰 Хайпанул, {amount} монет у {target} теперь твои!",
    "🦹‍♂️ Удачная кража! +{amount} от {target}",
    "😈 Ты краш, {target} даже не понял! +{amount}"
]

THEFT_FAIL_PHRASES = [
    "😢 Облом, тебя спалили! Ничего не украл.",
    "🚨 Треск, {target} оказался слишком бдительным!",
    "👮‍♂️ Пришлось сваливать, 0 монет.",
    "💔 Не фортануло, {target} слишком крутой."
]

THEFT_DEFENSE_PHRASES = [
    "🛡️ {target} отразил атаку! Ты потерял {penalty} монет.",
    "💥 Бабах! {target} выставил защиту, и ты лишился {penalty} монет.",
    "😱 Засада! Ты напоролся на защиту и потерял {penalty} монет."
]

THEFT_VICTIM_DEFENSE_PHRASES = [
    "🛡️ Твоя защита сработала! {attacker} ничего не украл и потерял {penalty} монет.",
    "💪 Ты краш! Отбил атаку {attacker} и получил {penalty} монет.",
    "😎 Ха! {attacker} думал поживиться, а сам потерял {penalty} монет."
]

# ===== КОМАНДА HELP =====
@dp.message_handler(commands=['help'])
async def cmd_help(message: types.Message):
    user_id = message.from_user.id
    if await is_banned(user_id) and not await is_admin(user_id):
        return
    ok, not_subscribed = await check_subscription(user_id)
    if not ok:
        await message.answer("❗️ Сначала подпишись на каналы.", reply_markup=subscription_inline(not_subscribed))
        return
    text = (
        "🤖 <b>Malboro GAME</b> – помощь:\n\n"
        "• 👤 Профиль – баланс и статистика\n"
        "• 🎁 Бонус – ежедневная награда\n"
        "• 🛒 Магазин подарков – покупка подарков\n"
        "• 🎰 Казино – испытай удачу\n"
        "• 🎟 Промокод – активация промокодов\n"
        "• 🏆 Топ игроков – лучшие по балансу\n"
        "• 💰 Мои покупки – история заказов\n"
        "• 🔫 Ограбить – укради монеты у другого\n\n"
        "Администраторы имеют дополнительные функции в панели."
    )
    await message.answer(text, reply_markup=user_main_keyboard(await is_admin(user_id)))

# ===== СТАРТ =====
@dp.message_handler(commands=['start'])
async def cmd_start(message: types.Message):
    user_id = message.from_user.id
    if await is_banned(user_id) and not await is_admin(user_id):
        await message.answer("⛔ Вы заблокированы.")
        return
    username = message.from_user.username
    first_name = message.from_user.first_name
    try:
        async with aiosqlite.connect(DB_PATH, timeout=10) as db:
            await db.execute(
                "INSERT OR IGNORE INTO users (user_id, username, first_name, joined_date, balance) VALUES (?, ?, ?, ?, ?)",
                (user_id, username, first_name, datetime.now().strftime("%Y-%m-%d %H:%M:%S"), 0)
            )
            await db.commit()
    except Exception as e:
        logging.error(f"DB error in start: {e}")
        await message.answer("❌ Ошибка базы данных. Попробуй позже.")
        return

    ok, not_subscribed = await check_subscription(user_id)
    if not ok:
        await message.answer(
            "❗️ Для доступа к боту нужно подписаться на наши каналы.\nПосле подписки нажми кнопку ниже.",
            reply_markup=subscription_inline(not_subscribed)
        )
        return
    admin_flag = await is_admin(user_id)
    await message.answer(
        f"Привет, {first_name}!\n"
        f"Добро пожаловать в <b>Malboro GAME</b>! 🚬\n"
        f"Тут ты найдёшь: казино, розыгрыши, магазин с подарками.\n"
        f"А ещё можешь грабить других (раз в 30 мин) – случайно или по username!\n\n"
        f"Канал: @lllMALBOROlll (подпишись, чтобы быть в теме)",
        reply_markup=user_main_keyboard(admin_flag)
    )

# ===== ПРОВЕРКА ПОДПИСКИ =====
@dp.callback_query_handler(lambda c: c.data == "check_sub")
async def check_sub_callback(callback: types.CallbackQuery):
    if await is_banned(callback.from_user.id) and not await is_admin(callback.from_user.id):
        await callback.answer("⛔ Вы заблокированы.", show_alert=True)
        return
    ok, not_subscribed = await check_subscription(callback.from_user.id)
    if ok:
        admin_flag = await is_admin(callback.from_user.id)
        await callback.message.edit_text("✅ Подписка подтверждена! Добро пожаловать.")
        await callback.message.answer("Главное меню:", reply_markup=user_main_keyboard(admin_flag))
    else:
        await callback.answer("❌ Ты ещё не подписался на все каналы!", show_alert=True)
        await callback.message.edit_reply_markup(reply_markup=subscription_inline(not_subscribed))

@dp.callback_query_handler(lambda c: c.data == "no_link")
async def no_link(callback: types.CallbackQuery):
    await callback.answer("Ссылка временно недоступна, найди канал вручную", show_alert=True)

# ===== ПРОФИЛЬ =====
@dp.message_handler(lambda message: message.text == "👤 Профиль")
async def profile_handler(message: types.Message):
    user_id = message.from_user.id
    if await is_banned(user_id) and not await is_admin(user_id):
        return
    ok, not_subscribed = await check_subscription(user_id)
    if not ok:
        await message.answer("❗️ Сначала подпишись на каналы.", reply_markup=subscription_inline(not_subscribed))
        return
    try:
        async with aiosqlite.connect(DB_PATH, timeout=10) as db:
            async with db.execute(
                "SELECT balance, joined_date, theft_attempts, theft_success, theft_failed, theft_protected FROM users WHERE user_id=?",
                (user_id,)
            ) as cursor:
                row = await cursor.fetchone()
        if row:
            balance, joined, attempts, success, failed, protected = row
            text = (
                f"👤 Твой профиль:\n"
                f"💰 Баланс: {balance} монет\n"
                f"📅 Зарегистрирован: {joined}\n"
                f"🔫 Ограблений: {attempts} (успешно: {success}, провал: {failed})\n"
                f"⚔️ Отбито атак: {protected}"
            )
        else:
            text = "Профиль не найден"
    except Exception as e:
        logging.error(f"Profile error: {e}")
        text = "❌ Ошибка загрузки профиля."
    await message.answer(text, reply_markup=user_main_keyboard(await is_admin(user_id)))

# ===== БОНУС =====
@dp.message_handler(lambda message: message.text == "🎁 Бонус")
async def bonus_handler(message: types.Message):
    user_id = message.from_user.id
    if await is_banned(user_id) and not await is_admin(user_id):
        return
    ok, not_subscribed = await check_subscription(user_id)
    if not ok:
        await message.answer("❗️ Сначала подпишись на каналы.", reply_markup=subscription_inline(not_subscribed))
        return
    try:
        async with aiosqlite.connect(DB_PATH, timeout=10) as db:
            async with db.execute("SELECT last_bonus FROM users WHERE user_id=?", (user_id,)) as cursor:
                row = await cursor.fetchone()
            last_bonus_str = row[0] if row else None

        now = datetime.now()
        if last_bonus_str:
            last_bonus = datetime.strptime(last_bonus_str, "%Y-%m-%d %H:%M:%S")
            if now - last_bonus < timedelta(days=1):
                remaining = timedelta(days=1) - (now - last_bonus)
                hours = remaining.seconds // 3600
                minutes = (remaining.seconds // 60) % 60
                await message.answer(f"⏳ Бонус можно будет получить через {hours} ч {minutes} мин")
                return

        bonus = random.randint(5, 15)
        phrase = random.choice(BONUS_PHRASES).format(bonus=bonus)

        async with aiosqlite.connect(DB_PATH, timeout=10) as db:
            await db.execute(
                "UPDATE users SET balance = balance + ?, last_bonus = ? WHERE user_id=?",
                (bonus, now.strftime("%Y-%m-%d %H:%M:%S"), user_id)
            )
            await db.commit()
        await message.answer(phrase, reply_markup=user_main_keyboard(await is_admin(user_id)))
    except Exception as e:
        logging.error(f"Bonus error: {e}")
        await message.answer("❌ Ошибка при получении бонуса.")

# ===== ТОП ИГРОКОВ =====
@dp.message_handler(lambda message: message.text == "🏆 Топ игроков")
async def leaderboard_handler(message: types.Message):
    user_id = message.from_user.id
    if await is_banned(user_id) and not await is_admin(user_id):
        return
    ok, not_subscribed = await check_subscription(user_id)
    if not ok:
        await message.answer("❗️ Сначала подпишись на каналы.", reply_markup=subscription_inline(not_subscribed))
        return
    try:
        async with aiosqlite.connect(DB_PATH, timeout=10) as db:
            async with db.execute(
                "SELECT first_name, balance FROM users ORDER BY balance DESC LIMIT 10"
            ) as cur:
                top = await cur.fetchall()
        if not top:
            await message.answer("Пока нет данных для топа.")
            return
        text = "🏆 <b>Топ 10 игроков по балансу:</b>\n\n"
        for idx, (name, bal) in enumerate(top, 1):
            text += f"{idx}. {name} – {bal} монет\n"
        await message.answer(text, reply_markup=user_main_keyboard(await is_admin(user_id)))
    except Exception as e:
        logging.error(f"Leaderboard error: {e}")
        await message.answer("❌ Ошибка загрузки топа.")

# ===== МАГАЗИН ПОДАРКОВ =====
@dp.message_handler(lambda message: message.text == "🛒 Магазин подарков")
async def shop_handler(message: types.Message):
    user_id = message.from_user.id
    if await is_banned(user_id) and not await is_admin(user_id):
        return
    ok, not_subscribed = await check_subscription(user_id)
    if not ok:
        await message.answer("❗️ Сначала подпишись на каналы.", reply_markup=subscription_inline(not_subscribed))
        return
    try:
        async with aiosqlite.connect(DB_PATH, timeout=10) as db:
            async with db.execute("SELECT id, name, description, price, stock FROM shop_items") as cursor:
                items = await cursor.fetchall()
        if not items:
            await message.answer("🎁 В магазине пока нет подарков.")
            return
        text = "🎁 Подарки:\n\n"
        kb = []
        for item in items:
            item_id, name, desc, price, stock = item
            stock_info = f" (в наличии: {stock})" if stock != -1 else ""
            text += f"🔹 {name}\n{desc}\n💰 {price} монет{stock_info}\n\n"
            kb.append([InlineKeyboardButton(text=f"Купить {name}", callback_data=f"buy_{item_id}")])
        await message.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))
    except Exception as e:
        logging.error(f"Shop error: {e}")
        await message.answer("❌ Ошибка загрузки магазина.")

@dp.callback_query_handler(lambda c: c.data.startswith("buy_"))
async def buy_callback(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    if await is_banned(user_id) and not await is_admin(user_id):
        await callback.answer("⛔ Вы заблокированы.", show_alert=True)
        return
    ok, not_subscribed = await check_subscription(user_id)
    if not ok:
        await callback.message.edit_text("❗️ Сначала подпишись на каналы.", reply_markup=subscription_inline(not_subscribed))
        return
    item_id = int(callback.data.split("_")[1])
    try:
        async with aiosqlite.connect(DB_PATH, timeout=10) as db:
            async with db.execute("SELECT name, price, stock FROM shop_items WHERE id=?", (item_id,)) as cursor:
                item = await cursor.fetchone()
            if not item:
                await callback.answer("Товар не найден", show_alert=True)
                return
            name, price, stock = item

            if stock != -1 and stock <= 0:
                await callback.answer("Товара нет в наличии!", show_alert=True)
                return

            async with db.execute("SELECT balance FROM users WHERE user_id=?", (user_id,)) as cursor:
                balance_row = await cursor.fetchone()
                if not balance_row:
                    await callback.answer("Пользователь не найден", show_alert=True)
                    return
                balance = balance_row[0]

            if balance < price:
                await callback.answer("Не хватает монет!", show_alert=True)
                return

            await db.execute("BEGIN")
            try:
                await db.execute("UPDATE users SET balance = balance - ? WHERE user_id=?", (price, user_id))
                await db.execute(
                    "INSERT INTO purchases (user_id, item_id, purchase_date) VALUES (?, ?, ?)",
                    (user_id, item_id, datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
                )
                if stock != -1:
                    await db.execute("UPDATE shop_items SET stock = stock - 1 WHERE id=?", (item_id,))
                await db.commit()
            except Exception as e:
                await db.rollback()
                raise e

        phrase = random.choice(PURCHASE_PHRASES)
        await callback.answer(f"✅ Ты купил {name}! {phrase}", show_alert=True)
        asyncio.create_task(notify_admins_about_purchase(callback.from_user, name, price))
        try:
            await callback.message.edit_text(f"✅ Покупка совершена!")
        except (MessageNotModified, MessageToEditNotFound):
            pass
        await callback.message.answer("Главное меню:", reply_markup=user_main_keyboard(await is_admin(user_id)))
    except Exception as e:
        logging.error(f"Purchase error: {e}")
        await callback.answer("❌ Ошибка при покупке. Попробуй позже.", show_alert=True)

async def notify_admins_about_purchase(user: types.User, item_name: str, price: int):
    admins = SUPER_ADMINS.copy()
    async with aiosqlite.connect(DB_PATH, timeout=10) as db:
        async with db.execute("SELECT user_id FROM admins") as cur:
            rows = await cur.fetchall()
            for row in rows:
                admins.append(row[0])
    for admin_id in admins:
        await safe_send_message(admin_id,
            f"🛒 Покупка: пользователь {user.full_name} (@{user.username})\n"
            f"<a href=\"tg://user?id={user.id}\">Ссылка</a> купил {item_name} за {price} монет."
        )

# ===== МОИ ПОКУПКИ =====
@dp.message_handler(lambda message: message.text == "💰 Мои покупки")
async def my_purchases(message: types.Message):
    user_id = message.from_user.id
    if await is_banned(user_id) and not await is_admin(user_id):
        return
    ok, not_subscribed = await check_subscription(user_id)
    if not ok:
        await message.answer("❗️ Сначала подпишись на каналы.", reply_markup=subscription_inline(not_subscribed))
        return
    try:
        async with aiosqlite.connect(DB_PATH, timeout=10) as db:
            async with db.execute(
                "SELECT p.id, s.name, p.purchase_date, p.status, p.admin_comment FROM purchases p JOIN shop_items s ON p.item_id = s.id WHERE p.user_id=? ORDER BY p.purchase_date DESC",
                (user_id,)
            ) as cursor:
                purchases = await cursor.fetchall()
        if not purchases:
            await message.answer("У тебя пока нет покупок.", reply_markup=user_main_keyboard(await is_admin(user_id)))
            return
        text = "📦 Твои покупки:\n"
        for pid, name, date, status, comment in purchases:
            status_emoji = "⏳" if status == 'pending' else "✅" if status == 'completed' else "❌"
            text += f"{status_emoji} {name} от {date}\n"
            if comment:
                text += f"   Комментарий: {comment}\n"
        await message.answer(text, reply_markup=user_main_keyboard(await is_admin(user_id)))
    except Exception as e:
        logging.error(f"My purchases error: {e}")
        await message.answer("❌ Ошибка загрузки покупок.")

# ===== КАЗИНО =====
@dp.message_handler(lambda message: message.text == "🎰 Казино")
async def casino_handler(message: types.Message):
    user_id = message.from_user.id
    if await is_banned(user_id) and not await is_admin(user_id):
        return
    ok, not_subscribed = await check_subscription(user_id)
    if not ok:
        await message.answer("❗️ Сначала подпишись на каналы.", reply_markup=subscription_inline(not_subscribed))
        return
    await message.answer("🎰 Введи сумму ставки (целое число):", reply_markup=back_keyboard())
    await CasinoBet.amount.set()

@dp.message_handler(state=CasinoBet.amount)
async def casino_bet_amount(message: types.Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.finish()
        await message.answer("Главное меню:", reply_markup=user_main_keyboard(await is_admin(message.from_user.id)))
        return
    try:
        amount = int(message.text)
    except:
        await message.answer("Введите число.")
        return
    if amount <= 0:
        await message.answer("Ставка должна быть положительной.")
        return
    user_id = message.from_user.id
    ok, not_subscribed = await check_subscription(user_id)
    if not ok:
        await message.answer("❗️ Сначала подпишись на каналы.", reply_markup=subscription_inline(not_subscribed))
        await state.finish()
        return
    try:
        win_chance = int(await get_setting("casino_win_chance")) / 100
        async with aiosqlite.connect(DB_PATH, timeout=10) as db:
            async with db.execute("SELECT balance FROM users WHERE user_id=?", (user_id,)) as cursor:
                balance = (await cursor.fetchone())[0]
            if amount > balance:
                await message.answer("Недостаточно монет.")
                await state.finish()
                return
            win = random.random() < win_chance
            if win:
                await db.execute("UPDATE users SET balance = balance + ? WHERE user_id=?", (amount, user_id))
                profit = amount
                win_amount = amount * 2
                phrase = random.choice(CASINO_WIN_PHRASES).format(win=win_amount, profit=profit)
            else:
                await db.execute("UPDATE users SET balance = balance - ? WHERE user_id=?", (amount, user_id))
                phrase = random.choice(CASINO_LOSE_PHRASES).format(loss=amount)
            await db.commit()
            async with db.execute("SELECT balance FROM users WHERE user_id=?", (user_id,)) as cursor:
                new_balance = (await cursor.fetchone())[0]
        await message.answer(
            f"{phrase}\n💰 Текущий баланс: {new_balance}",
            reply_markup=user_main_keyboard(await is_admin(user_id))
        )
    except Exception as e:
        logging.error(f"Casino error: {e}")
        await message.answer("❌ Ошибка в казино.")
    await state.finish()

# ===== ПРОМОКОД =====
@dp.message_handler(lambda message: message.text == "🎟 Промокод")
async def promo_handler(message: types.Message):
    user_id = message.from_user.id
    if await is_banned(user_id) and not await is_admin(user_id):
        return
    ok, not_subscribed = await check_subscription(user_id)
    if not ok:
        await message.answer("❗️ Сначала подпишись на каналы.", reply_markup=subscription_inline(not_subscribed))
        return
    await message.answer("Введи промокод:", reply_markup=back_keyboard())
    await PromoActivate.code.set()

@dp.message_handler(state=PromoActivate.code)
async def promo_activate(message: types.Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.finish()
        await message.answer("Главное меню:", reply_markup=user_main_keyboard(await is_admin(message.from_user.id)))
        return
    code = message.text.strip().upper()
    user_id = message.from_user.id
    ok, not_subscribed = await check_subscription(user_id)
    if not ok:
        await message.answer("❗️ Сначала подпишись на каналы.", reply_markup=subscription_inline(not_subscribed))
        await state.finish()
        return
    try:
        async with aiosqlite.connect(DB_PATH, timeout=10) as db:
            async with db.execute("SELECT reward, max_uses, used_count FROM promocodes WHERE code=?", (code,)) as cursor:
                row = await cursor.fetchone()
            if not row:
                await message.answer("❌ Промокод не найден.")
                await state.finish()
                return
            reward, max_uses, used = row
            if used >= max_uses:
                await message.answer("❌ Промокод уже использован максимальное количество раз.")
                await state.finish()
                return
            await db.execute("UPDATE users SET balance = balance + ? WHERE user_id=?", (reward, user_id))
            await db.execute("UPDATE promocodes SET used_count = used_count + 1 WHERE code=?", (code,))
            await db.commit()
        await message.answer(
            f"✅ Промокод активирован! Ты получил {reward} монет.",
            reply_markup=user_main_keyboard(await is_admin(user_id))
        )
    except Exception as e:
        logging.error(f"Promo error: {e}")
        await message.answer("❌ Ошибка активации промокода.")
    await state.finish()

# ===== РОЗЫГРЫШИ =====
@dp.message_handler(lambda message: message.text == "🎲 Розыгрыши")
async def giveaways_handler(message: types.Message):
    user_id = message.from_user.id
    if await is_banned(user_id) and not await is_admin(user_id):
        return
    ok, not_subscribed = await check_subscription(user_id)
    if not ok:
        await message.answer("❗️ Сначала подпишись на каналы.", reply_markup=subscription_inline(not_subscribed))
        return
    try:
        async with aiosqlite.connect(DB_PATH, timeout=10) as db:
            async with db.execute("SELECT id, prize, end_date FROM giveaways WHERE status='active'") as cursor:
                rows = await cursor.fetchall()
        if not rows:
            await message.answer(
                "Сейчас нет активных розыгрышей.",
                reply_markup=user_main_keyboard(await is_admin(user_id))
            )
            return
        text = "🎁 Активные розыгрыши:\n\n"
        kb = []
        for row in rows:
            gid, prize, end = row
            async with aiosqlite.connect(DB_PATH, timeout=10) as db2:
                async with db2.execute("SELECT COUNT(*) FROM participants WHERE giveaway_id=?", (gid,)) as cur:
                    count = (await cur.fetchone())[0]
            text += f"ID: {gid} | {prize} | до {end} | 👥 {count} участников\n"
            kb.append([InlineKeyboardButton(text=f"🔍 Подробнее о {prize}", callback_data=f"detail_{gid}")])
        kb.append([InlineKeyboardButton(text="« Назад", callback_data="back_main")])
        await message.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))
    except Exception as e:
        logging.error(f"Giveaways list error: {e}")
        await message.answer("❌ Ошибка загрузки розыгрышей.")

@dp.callback_query_handler(lambda c: c.data.startswith("detail_"))
async def giveaway_detail(callback: types.CallbackQuery):
    if await is_banned(callback.from_user.id) and not await is_admin(callback.from_user.id):
        await callback.answer("⛔ Вы заблокированы.", show_alert=True)
        return
    giveaway_id = int(callback.data.split("_")[1])
    try:
        async with aiosqlite.connect(DB_PATH, timeout=10) as db:
            async with db.execute(
                "SELECT prize, description, end_date, media_file_id, media_type FROM giveaways WHERE id=? AND status='active'",
                (giveaway_id,)
            ) as cursor:
                row = await cursor.fetchone()
            async with db.execute("SELECT COUNT(*) FROM participants WHERE giveaway_id=?", (giveaway_id,)) as cur:
                participants_count = (await cur.fetchone())[0]
        if not row:
            await callback.answer("Розыгрыш не найден или завершён.", show_alert=True)
            return
        prize, desc, end_date, media_file_id, media_type = row
        caption = f"🎁 Розыгрыш: {prize}\n📝 {desc}\n📅 Окончание: {end_date}\n👥 Участников: {participants_count}\n\nЖелаешь участвовать?"
        confirm_kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ Да, участвую", callback_data=f"confirm_part_{giveaway_id}")],
            [InlineKeyboardButton(text="❌ Нет", callback_data="cancel_detail")]
        ])
        if media_file_id and media_type:
            if media_type == 'photo':
                await callback.message.answer_photo(photo=media_file_id, caption=caption, reply_markup=confirm_kb)
            elif media_type == 'video':
                await callback.message.answer_video(video=media_file_id, caption=caption, reply_markup=confirm_kb)
            elif media_type == 'document':
                await callback.message.answer_document(document=media_file_id, caption=caption, reply_markup=confirm_kb)
        else:
            await callback.message.answer(caption, reply_markup=confirm_kb)
        await callback.answer()
    except Exception as e:
        logging.error(f"Giveaway detail error: {e}")
        await callback.answer("Ошибка загрузки деталей.", show_alert=True)

@dp.callback_query_handler(lambda c: c.data.startswith("confirm_part_"))
async def confirm_participation(callback: types.CallbackQuery):
    if await is_banned(callback.from_user.id) and not await is_admin(callback.from_user.id):
        await callback.answer("⛔ Вы заблокированы.", show_alert=True)
        return
    giveaway_id = int(callback.data.split("_")[2])
    user_id = callback.from_user.id
    ok, not_subscribed = await check_subscription(user_id)
    if not ok:
        await callback.message.edit_text("❗️ Сначала подпишись на каналы.", reply_markup=subscription_inline(not_subscribed))
        return
    try:
        async with aiosqlite.connect(DB_PATH, timeout=10) as db:
            async with db.execute("SELECT status FROM giveaways WHERE id=?", (giveaway_id,)) as cursor:
                row = await cursor.fetchone()
                if not row or row[0] != 'active':
                    await callback.answer("Розыгрыш не активен", show_alert=True)
                    return
            await db.execute("INSERT OR IGNORE INTO participants (user_id, giveaway_id) VALUES (?, ?)", (user_id, giveaway_id))
            await db.commit()
        await callback.answer("✅ Ты участвуешь в розыгрыше!", show_alert=True)
        await giveaways_handler(callback.message)
    except Exception as e:
        logging.error(f"Participation error: {e}")
        await callback.answer("Ошибка при участии.", show_alert=True)

@dp.callback_query_handler(lambda c: c.data == "cancel_detail")
async def cancel_detail(callback: types.CallbackQuery):
    if await is_banned(callback.from_user.id) and not await is_admin(callback.from_user.id):
        return
    await callback.message.delete()
    await giveaways_handler(callback.message)

@dp.callback_query_handler(lambda c: c.data == "back_main")
async def back_main_callback(callback: types.CallbackQuery):
    if await is_banned(callback.from_user.id) and not await is_admin(callback.from_user.id):
        return
    admin_flag = await is_admin(callback.from_user.id)
    await callback.message.delete()
    await callback.message.answer("Главное меню:", reply_markup=user_main_keyboard(admin_flag))

# ===== ОГРАБЛЕНИЕ =====
@dp.message_handler(lambda message: message.text == "🔫 Ограбить")
async def theft_menu(message: types.Message):
    user_id = message.from_user.id
    if await is_banned(user_id) and not await is_admin(user_id):
        return
    ok, not_subscribed = await check_subscription(user_id)
    if not ok:
        await message.answer("❗️ Сначала подпишись на каналы.", reply_markup=subscription_inline(not_subscribed))
        return
    phrase = random.choice(THEFT_CHOICE_PHRASES)
    await message.answer(phrase, reply_markup=theft_choice_keyboard())

@dp.message_handler(lambda message: message.text == "🎲 Случайная цель")
async def theft_random(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    cooldown_minutes = int(await get_setting("theft_cooldown_minutes"))
    async with aiosqlite.connect(DB_PATH, timeout=10) as db:
        async with db.execute("SELECT last_theft_time FROM users WHERE user_id=?", (user_id,)) as cur:
            row = await cur.fetchone()
            if row and row[0]:
                last_time = datetime.strptime(row[0], "%Y-%m-%d %H:%M:%S")
                diff = datetime.now() - last_time
                if diff < timedelta(minutes=cooldown_minutes):
                    remaining = cooldown_minutes - int(diff.total_seconds() // 60)
                    phrase = random.choice(THEFT_COOLDOWN_PHRASES).format(minutes=remaining)
                    await message.answer(phrase, reply_markup=user_main_keyboard(await is_admin(user_id)))
                    return
    target_id = await get_random_user(user_id)
    if not target_id:
        await message.answer("😕 В игре пока нет других игроков.", reply_markup=user_main_keyboard(await is_admin(user_id)))
        return
    cost = int(await get_setting("random_attack_cost"))
    if cost > 0:
        balance = await get_user_balance(user_id)
        if balance < cost:
            await message.answer(random.choice(THEFT_NO_MONEY_PHRASES), reply_markup=user_main_keyboard(await is_admin(user_id)))
            return
        await update_user_balance(user_id, -cost)
    await perform_theft(message, user_id, target_id, is_random=True)

@dp.message_handler(lambda message: message.text == "👤 Выбрать пользователя")
async def theft_choose_user(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    cooldown_minutes = int(await get_setting("theft_cooldown_minutes"))
    async with aiosqlite.connect(DB_PATH, timeout=10) as db:
        async with db.execute("SELECT last_theft_time FROM users WHERE user_id=?", (user_id,)) as cur:
            row = await cur.fetchone()
            if row and row[0]:
                last_time = datetime.strptime(row[0], "%Y-%m-%d %H:%M:%S")
                diff = datetime.now() - last_time
                if diff < timedelta(minutes=cooldown_minutes):
                    remaining = cooldown_minutes - int(diff.total_seconds() // 60)
                    phrase = random.choice(THEFT_COOLDOWN_PHRASES).format(minutes=remaining)
                    await message.answer(phrase, reply_markup=user_main_keyboard(await is_admin(user_id)))
                    return
    await message.answer("Введи @username или ID того, кого хочешь ограбить:", reply_markup=back_keyboard())
    await TheftTarget.target.set()

@dp.message_handler(state=TheftTarget.target)
async def theft_target_entered(message: types.Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.finish()
        await message.answer("Главное меню:", reply_markup=user_main_keyboard(await is_admin(message.from_user.id)))
        return
    target_input = message.text.strip()
    robber_id = message.from_user.id

    target_id = None
    if target_input.startswith('@'):
        username = target_input[1:].lower()
        async with aiosqlite.connect(DB_PATH, timeout=10) as db:
            async with db.execute("SELECT user_id FROM users WHERE LOWER(username)=?", (username,)) as cur:
                row = await cur.fetchone()
                if row:
                    target_id = row[0]
    else:
        try:
            target_id = int(target_input)
        except ValueError:
            async with aiosqlite.connect(DB_PATH, timeout=10) as db:
                async with db.execute("SELECT user_id FROM users WHERE LOWER(username)=?", (target_input.lower(),)) as cur:
                    row = await cur.fetchone()
                    if row:
                        target_id = row[0]

    if not target_id:
        await message.answer("❌ Пользователь не найден. Проверь username или ID.")
        return

    if target_id == robber_id:
        await message.answer("Сам себя не ограбишь, бро! 😆")
        await state.finish()
        return

    if await is_banned(target_id):
        await message.answer("❌ Этот пользователь заблокирован и не может быть целью.")
        await state.finish()
        return

    cost = int(await get_setting("targeted_attack_cost"))
    if cost > 0:
        balance = await get_user_balance(robber_id)
        if balance < cost:
            await message.answer(random.choice(THEFT_NO_MONEY_PHRASES), reply_markup=user_main_keyboard(await is_admin(robber_id)))
            await state.finish()
            return
        await update_user_balance(robber_id, -cost)

    await perform_theft(message, robber_id, target_id, is_random=False)
    await state.finish()

async def perform_theft(message: types.Message, robber_id: int, victim_id: int, is_random: bool):
    try:
        async with aiosqlite.connect(DB_PATH, timeout=10) as db:
            async with db.execute("SELECT balance FROM users WHERE user_id=?", (victim_id,)) as cur:
                victim_balance_row = await cur.fetchone()
                if not victim_balance_row:
                    await message.answer("❌ Цель не найдена в базе.")
                    return
                victim_balance = victim_balance_row[0]

            success_chance = int(await get_setting("theft_success_chance"))
            defense_chance = int(await get_setting("theft_defense_chance"))
            defense_penalty = int(await get_setting("theft_defense_penalty"))
            min_amount = int(await get_setting("min_theft_amount"))
            max_amount = int(await get_setting("max_theft_amount"))

            async with db.execute("SELECT username, first_name FROM users WHERE user_id=?", (victim_id,)) as cur:
                victim_info = await cur.fetchone()
                victim_name = victim_info[1] if victim_info else str(victim_id)

            defense_triggered = random.randint(1, 100) <= defense_chance
            if defense_triggered:
                penalty = defense_penalty
                robber_balance = await get_user_balance(robber_id)
                if penalty > robber_balance:
                    penalty = robber_balance
                if penalty > 0:
                    await db.execute("UPDATE users SET balance = balance - ? WHERE user_id=?", (penalty, robber_id))
                    await db.execute("UPDATE users SET balance = balance + ? WHERE user_id=?", (penalty, victim_id))
                await db.execute("UPDATE users SET theft_attempts = theft_attempts + 1, theft_failed = theft_failed + 1 WHERE user_id=?", (robber_id,))
                await db.execute("UPDATE users SET theft_protected = theft_protected + 1 WHERE user_id=?", (victim_id,))
                await db.execute("UPDATE users SET last_theft_time = ? WHERE user_id=?", (datetime.now().strftime("%Y-%m-%d %H:%M:%S"), robber_id))
                await db.commit()

                robber_phrase = random.choice(THEFT_DEFENSE_PHRASES).format(target=victim_name, penalty=penalty)
                victim_phrase = random.choice(THEFT_VICTIM_DEFENSE_PHRASES).format(attacker=message.from_user.first_name, penalty=penalty)
                await message.answer(robber_phrase, reply_markup=user_main_keyboard(await is_admin(robber_id)))
                await safe_send_message(victim_id, victim_phrase)
                return

            success = random.randint(1, 100) <= success_chance
            if success and victim_balance > 0:
                steal_amount = random.randint(min_amount, min(max_amount, victim_balance))
                await db.execute("UPDATE users SET balance = balance - ? WHERE user_id=?", (steal_amount, victim_id))
                await db.execute("UPDATE users SET balance = balance + ? WHERE user_id=?", (steal_amount, robber_id))
                await db.execute("UPDATE users SET theft_attempts = theft_attempts + 1, theft_success = theft_success + 1 WHERE user_id=?", (robber_id,))
                phrase = random.choice(THEFT_SUCCESS_PHRASES).format(amount=steal_amount, target=victim_name)
                await message.answer(phrase, reply_markup=user_main_keyboard(await is_admin(robber_id)))
                await safe_send_message(victim_id, f"🔫 Вас ограбили! {message.from_user.first_name} украл {steal_amount} монет.")
            else:
                await db.execute("UPDATE users SET theft_attempts = theft_attempts + 1, theft_failed = theft_failed + 1 WHERE user_id=?", (robber_id,))
                phrase = random.choice(THEFT_FAIL_PHRASES).format(target=victim_name)
                await message.answer(phrase, reply_markup=user_main_keyboard(await is_admin(robber_id)))

            await db.execute("UPDATE users SET last_theft_time = ? WHERE user_id=?", (datetime.now().strftime("%Y-%m-%d %H:%M:%S"), robber_id))
            await db.commit()

    except Exception as e:
        logging.error(f"Theft error: {e}")
        await message.answer("❌ Ошибка при ограблении.")

# ===== АДМИН ПАНЕЛЬ =====
@dp.message_handler(lambda message: message.text == "⚙️ Админ панель")
async def admin_panel(message: types.Message):
    if not await is_admin(message.from_user.id):
        await message.answer("У тебя нет прав администратора.")
        return
    super_admin = await is_super_admin(message.from_user.id)
    await message.answer("Панель администратора:", reply_markup=admin_main_keyboard(super_admin))

# ===== УПРАВЛЕНИЕ РОЗЫГРЫШАМИ =====
@dp.message_handler(lambda message: message.text == "🎁 Управление розыгрышами")
async def admin_giveaway_menu(message: types.Message):
    if not await is_admin(message.from_user.id):
        return
    await message.answer("Управление розыгрышами:", reply_markup=giveaway_admin_keyboard())

@dp.message_handler(lambda message: message.text == "➕ Создать розыгрыш")
async def create_giveaway_start(message: types.Message):
    if not await is_admin(message.from_user.id):
        return
    await message.answer("Введи название приза:", reply_markup=back_keyboard())
    await CreateGiveaway.prize.set()

@dp.message_handler(state=CreateGiveaway.prize)
async def create_giveaway_prize(message: types.Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.finish()
        await admin_giveaway_menu(message)
        return
    await state.update_data(prize=message.text)
    await message.answer("Введи описание розыгрыша:")
    await CreateGiveaway.description.set()

@dp.message_handler(state=CreateGiveaway.description)
async def create_giveaway_description(message: types.Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.finish()
        await admin_giveaway_menu(message)
        return
    await state.update_data(description=message.text)
    await message.answer("Введи дату окончания в формате ДД.ММ.ГГГГ ЧЧ:ММ (например, 31.12.2025 23:59):")
    await CreateGiveaway.end_date.set()

@dp.message_handler(state=CreateGiveaway.end_date)
async def create_giveaway_end_date(message: types.Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.finish()
        await admin_giveaway_menu(message)
        return
    try:
        end_date = datetime.strptime(message.text, "%d.%m.%Y %H:%M")
        if end_date <= datetime.now():
            await message.answer("Дата окончания должна быть в будущем.")
            return
        await state.update_data(end_date=end_date.strftime("%Y-%m-%d %H:%M:%S"))
    except ValueError:
        await message.answer("Неверный формат. Используй ДД.ММ.ГГГГ ЧЧ:ММ")
        return
    await message.answer("Отправь медиа (фото, видео или документ) для розыгрыша или отправь 'пропустить':")
    await CreateGiveaway.media.set()

@dp.message_handler(state=CreateGiveaway.media, content_types=['text', 'photo', 'video', 'document'])
async def create_giveaway_media(message: types.Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.finish()
        await admin_giveaway_menu(message)
        return
    data = await state.get_data()
    media_file_id = None
    media_type = None
    if message.photo:
        media_file_id = message.photo[-1].file_id
        media_type = 'photo'
    elif message.video:
        media_file_id = message.video.file_id
        media_type = 'video'
    elif message.document:
        media_file_id = message.document.file_id
        media_type = 'document'
    elif message.text and message.text.lower() == 'пропустить':
        pass
    else:
        await message.answer("Пожалуйста, отправь фото, видео, документ или 'пропустить'.")
        return

    try:
        async with aiosqlite.connect(DB_PATH, timeout=10) as db:
            await db.execute(
                "INSERT INTO giveaways (prize, description, end_date, media_file_id, media_type) VALUES (?, ?, ?, ?, ?)",
                (data['prize'], data['description'], data['end_date'], media_file_id, media_type)
            )
            await db.commit()
        await message.answer("✅ Розыгрыш создан!", reply_markup=giveaway_admin_keyboard())
    except Exception as e:
        logging.error(f"Create giveaway error: {e}")
        await message.answer("❌ Ошибка при создании розыгрыша.")
    await state.finish()

@dp.message_handler(lambda message: message.text == "📋 Активные розыгрыши")
async def list_active_giveaways(message: types.Message):
    if not await is_admin(message.from_user.id):
        return
    try:
        async with aiosqlite.connect(DB_PATH, timeout=10) as db:
            async with db.execute("SELECT id, prize, end_date, description FROM giveaways WHERE status='active'") as cursor:
                rows = await cursor.fetchall()
        if not rows:
            await message.answer("Нет активных розыгрышей.")
            return
        text = "Активные розыгрыши:\n"
        for gid, prize, end, desc in rows:
            async with aiosqlite.connect(DB_PATH, timeout=10) as db2:
                async with db2.execute("SELECT COUNT(*) FROM participants WHERE giveaway_id=?", (gid,)) as cur:
                    count = (await cur.fetchone())[0]
            text += f"ID: {gid} | {prize} | до {end} | 👥 {count} участников\n{desc}\n\n"
        await message.answer(text, reply_markup=giveaway_admin_keyboard())
    except Exception as e:
        logging.error(f"List giveaways error: {e}")
        await message.answer("❌ Ошибка.")

@dp.message_handler(lambda message: message.text == "✅ Завершить розыгрыш")
async def finish_giveaway_start(message: types.Message):
    if not await is_admin(message.from_user.id):
        return
    await message.answer("Введи ID розыгрыша, который нужно завершить:", reply_markup=back_keyboard())
    await CompleteGiveaway.giveaway_id.set()

@dp.message_handler(state=CompleteGiveaway.giveaway_id)
async def finish_giveaway(message: types.Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.finish()
        await admin_giveaway_menu(message)
        return
    try:
        gid = int(message.text)
    except:
        await message.answer("Введи число.")
        return
    await state.update_data(giveaway_id=gid)
    await message.answer("Введи количество победителей (целое число):")
    await CompleteGiveaway.winners_count.set()

@dp.message_handler(state=CompleteGiveaway.winners_count)
async def finish_giveaway_winners(message: types.Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.finish()
        await admin_giveaway_menu(message)
        return
    try:
        winners_count = int(message.text)
        if winners_count < 1:
            raise ValueError
    except:
        await message.answer("Введи положительное целое число.")
        return
    data = await state.get_data()
    gid = data['giveaway_id']
    try:
        async with aiosqlite.connect(DB_PATH, timeout=10) as db:
            async with db.execute("SELECT status FROM giveaways WHERE id=?", (gid,)) as cur:
                row = await cur.fetchone()
                if not row or row[0] != 'active':
                    await message.answer("Розыгрыш не активен или не существует.")
                    await state.finish()
                    return
            async with db.execute("SELECT user_id FROM participants WHERE giveaway_id=?", (gid,)) as cur:
                participants = [row[0] for row in await cur.fetchall()]
            if not participants:
                await message.answer("В этом розыгрыше нет участников.")
                await state.finish()
                return
            if winners_count > len(participants):
                winners_count = len(participants)
            winners = random.sample(participants, winners_count)
            await db.execute("UPDATE giveaways SET status='completed', winner_id=? WHERE id=?", (winners[0], gid))
            for wid in winners:
                safe_send_message_task(wid, f"🎉 Поздравляем! Ты выиграл в розыгрыше! Свяжись с админом.")
            await db.commit()
        await message.answer(f"🏆 Победители выбраны! ({len(winners)})", reply_markup=giveaway_admin_keyboard())
    except Exception as e:
        logging.error(f"Finish giveaway error: {e}")
        await message.answer("❌ Ошибка.")
    await state.finish()

# ===== УПРАВЛЕНИЕ КАНАЛАМИ =====
@dp.message_handler(lambda message: message.text == "📺 Управление каналами")
async def admin_channel_menu(message: types.Message):
    if not await is_admin(message.from_user.id):
        return
    await message.answer("Управление каналами:", reply_markup=channel_admin_keyboard())

@dp.message_handler(lambda message: message.text == "➕ Добавить канал")
async def add_channel_start(message: types.Message):
    if not await is_admin(message.from_user.id):
        return
    await message.answer("Введи chat_id канала (можно получить у @username_to_id_bot):", reply_markup=back_keyboard())
    await AddChannel.chat_id.set()

@dp.message_handler(state=AddChannel.chat_id)
async def add_channel_chat_id(message: types.Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.finish()
        await admin_channel_menu(message)
        return
    await state.update_data(chat_id=message.text.strip())
    await message.answer("Введи название канала:")
    await AddChannel.next()

@dp.message_handler(state=AddChannel.title)
async def add_channel_title(message: types.Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.finish()
        await admin_channel_menu(message)
        return
    await state.update_data(title=message.text)
    await message.answer("Введи invite-ссылку (или отправь 'нет'):")
    await AddChannel.next()

@dp.message_handler(state=AddChannel.invite_link)
async def add_channel_link(message: types.Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.finish()
        await admin_channel_menu(message)
        return
    link = None if message.text.lower() == 'нет' else message.text.strip()
    data = await state.get_data()
    try:
        async with aiosqlite.connect(DB_PATH, timeout=10) as db:
            await db.execute(
                "INSERT INTO channels (chat_id, title, invite_link) VALUES (?, ?, ?)",
                (data['chat_id'], data['title'], link)
            )
            await db.commit()
        await message.answer("✅ Канал добавлен!", reply_markup=channel_admin_keyboard())
    except aiosqlite.IntegrityError:
        await message.answer("❌ Канал с таким chat_id уже существует.")
    except Exception as e:
        logging.error(f"Add channel error: {e}")
        await message.answer("❌ Ошибка.")
    await state.finish()

@dp.message_handler(lambda message: message.text == "➖ Удалить канал")
async def remove_channel_start(message: types.Message):
    if not await is_admin(message.from_user.id):
        return
    await message.answer("Введи chat_id канала для удаления:", reply_markup=back_keyboard())
    await RemoveChannel.chat_id.set()

@dp.message_handler(state=RemoveChannel.chat_id)
async def remove_channel(message: types.Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.finish()
        await admin_channel_menu(message)
        return
    chat_id = message.text.strip()
    try:
        async with aiosqlite.connect(DB_PATH, timeout=10) as db:
            await db.execute("DELETE FROM channels WHERE chat_id=?", (chat_id,))
            await db.commit()
        await message.answer("✅ Канал удалён, если существовал.", reply_markup=channel_admin_keyboard())
    except Exception as e:
        logging.error(f"Remove channel error: {e}")
        await message.answer("❌ Ошибка.")
    await state.finish()

@dp.message_handler(lambda message: message.text == "📋 Список каналов")
async def list_channels(message: types.Message):
    if not await is_admin(message.from_user.id):
        return
    channels = await get_channels()
    if not channels:
        await message.answer("Нет добавленных каналов.")
        return
    text = "📺 Каналы для подписки:\n"
    for chat_id, title, link in channels:
        text += f"• {title} (chat_id: {chat_id})\n  Ссылка: {link or 'нет'}\n"
    await message.answer(text, reply_markup=channel_admin_keyboard())

# ===== УПРАВЛЕНИЕ МАГАЗИНОМ =====
@dp.message_handler(lambda message: message.text == "🛒 Управление магазином")
async def admin_shop_menu(message: types.Message):
    if not await is_admin(message.from_user.id):
        return
    await message.answer("Управление магазином:", reply_markup=shop_admin_keyboard())

@dp.message_handler(lambda message: message.text == "➕ Добавить товар")
async def add_shop_item_start(message: types.Message):
    if not await is_admin(message.from_user.id):
        return
    await message.answer("Введи название товара:", reply_markup=back_keyboard())
    await AddShopItem.name.set()

@dp.message_handler(state=AddShopItem.name)
async def add_shop_item_name(message: types.Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.finish()
        await admin_shop_menu(message)
        return
    await state.update_data(name=message.text)
    await message.answer("Введи описание товара:")
    await AddShopItem.next()

@dp.message_handler(state=AddShopItem.description)
async def add_shop_item_description(message: types.Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.finish()
        await admin_shop_menu(message)
        return
    await state.update_data(description=message.text)
    await message.answer("Введи цену (целое число):")
    await AddShopItem.next()

@dp.message_handler(state=AddShopItem.price)
async def add_shop_item_price(message: types.Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.finish()
        await admin_shop_menu(message)
        return
    try:
        price = int(message.text)
        if price <= 0:
            raise ValueError
    except:
        await message.answer("Цена должна быть положительным целым числом.")
        return
    await state.update_data(price=price)
    await message.answer("Введи количество товара (целое число, -1 для бесконечного):")
    await AddShopItem.stock.set()

@dp.message_handler(state=AddShopItem.stock)
async def add_shop_item_stock(message: types.Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.finish()
        await admin_shop_menu(message)
        return
    try:
        stock = int(message.text)
    except:
        await message.answer("Введи целое число.")
        return
    data = await state.get_data()
    try:
        async with aiosqlite.connect(DB_PATH, timeout=10) as db:
            await db.execute(
                "INSERT INTO shop_items (name, description, price, stock) VALUES (?, ?, ?, ?)",
                (data['name'], data['description'], data['price'], stock)
            )
            await db.commit()
        await message.answer("✅ Товар добавлен!", reply_markup=shop_admin_keyboard())
    except Exception as e:
        logging.error(f"Add shop item error: {e}")
        await message.answer("❌ Ошибка при добавлении товара.")
    await state.finish()

@dp.message_handler(lambda message: message.text == "➖ Удалить товар")
async def remove_shop_item_start(message: types.Message):
    if not await is_admin(message.from_user.id):
        return
    try:
        async with aiosqlite.connect(DB_PATH, timeout=10) as db:
            async with db.execute("SELECT id, name FROM shop_items ORDER BY id") as cur:
                items = await cur.fetchall()
        if not items:
            await message.answer("В магазине нет товаров.")
            return
        text = "Товары:\n" + "\n".join([f"ID {i[0]}: {i[1]}" for i in items])
        await message.answer(text + "\n\nВведи ID товара для удаления:", reply_markup=back_keyboard())
    except Exception as e:
        logging.error(f"List items for remove error: {e}")
        await message.answer("❌ Ошибка.")
        return
    await RemoveShopItem.item_id.set()

@dp.message_handler(state=RemoveShopItem.item_id)
async def remove_shop_item(message: types.Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.finish()
        await admin_shop_menu(message)
        return
    try:
        item_id = int(message.text)
    except:
        await message.answer("Введи число.")
        return
    try:
        async with aiosqlite.connect(DB_PATH, timeout=10) as db:
            await db.execute("DELETE FROM shop_items WHERE id=?", (item_id,))
            await db.commit()
        await message.answer("✅ Товар удалён, если существовал.", reply_markup=shop_admin_keyboard())
    except Exception as e:
        logging.error(f"Remove shop item error: {e}")
        await message.answer("❌ Ошибка.")
    await state.finish()

@dp.message_handler(lambda message: message.text == "📋 Список товаров")
async def list_shop_items(message: types.Message):
    if not await is_admin(message.from_user.id):
        return
    try:
        async with aiosqlite.connect(DB_PATH, timeout=10) as db:
            async with db.execute("SELECT id, name, description, price, stock FROM shop_items ORDER BY id") as cur:
                items = await cur.fetchall()
        if not items:
            await message.answer("В магазине нет товаров.")
            return
        text = "📦 Товары:\n"
        for item in items:
            item_id, name, desc, price, stock = item
            text += f"\nID {item_id} | {name}\n{desc}\n💰 {price} | наличие: {stock if stock!=-1 else '∞'}\n"
        await message.answer(text, reply_markup=shop_admin_keyboard())
    except Exception as e:
        logging.error(f"List shop items error: {e}")
        await message.answer("❌ Ошибка.")

@dp.message_handler(lambda message: message.text == "✏️ Редактировать товар")
async def edit_shop_item_start(message: types.Message):
    if not await is_admin(message.from_user.id):
        return
    await message.answer("Введи ID товара для редактирования:", reply_markup=back_keyboard())
    await EditShopItem.item_id.set()

@dp.message_handler(state=EditShopItem.item_id)
async def edit_shop_item_field(message: types.Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.finish()
        await admin_shop_menu(message)
        return
    try:
        item_id = int(message.text)
    except:
        await message.answer("Введи число.")
        return
    await state.update_data(item_id=item_id)
    await message.answer("Что хочешь изменить? (price/stock)", reply_markup=back_keyboard())
    await EditShopItem.field.set()

@dp.message_handler(state=EditShopItem.field)
async def edit_shop_item_value(message: types.Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.finish()
        await admin_shop_menu(message)
        return
    field = message.text.lower()
    if field not in ['price', 'stock']:
        await message.answer("Можно изменить только price или stock.")
        return
    await state.update_data(field=field)
    await message.answer(f"Введи новое значение для {field}:")
    await EditShopItem.value.set()

@dp.message_handler(state=EditShopItem.value)
async def edit_shop_item_final(message: types.Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.finish()
        await admin_shop_menu(message)
        return
    try:
        value = int(message.text)
    except:
        await message.answer("Введи целое число.")
        return
    data = await state.get_data()
    item_id = data['item_id']
    field = data['field']
    try:
        async with aiosqlite.connect(DB_PATH, timeout=10) as db:
            await db.execute(f"UPDATE shop_items SET {field}=? WHERE id=?", (value, item_id))
            await db.commit()
        await message.answer("✅ Товар обновлён.", reply_markup=shop_admin_keyboard())
    except Exception as e:
        logging.error(f"Edit shop item error: {e}")
        await message.answer("❌ Ошибка.")
    await state.finish()

# ===== УПРАВЛЕНИЕ ПРОМОКОДАМИ =====
@dp.message_handler(lambda message: message.text == "🎫 Управление промокодами")
async def admin_promo_menu(message: types.Message):
    if not await is_admin(message.from_user.id):
        return
    await message.answer("Управление промокодами:", reply_markup=promo_admin_keyboard())

@dp.message_handler(lambda message: message.text == "➕ Создать промокод")
async def create_promo_start(message: types.Message):
    if not await is_admin(message.from_user.id):
        return
    await message.answer("Введи код промокода (латиница, цифры):", reply_markup=back_keyboard())
    await CreatePromocode.code.set()

@dp.message_handler(state=CreatePromocode.code)
async def create_promo_code(message: types.Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.finish()
        await admin_promo_menu(message)
        return
    code = message.text.strip().upper()
    await state.update_data(code=code)
    await message.answer("Введи количество монет, которые даёт промокод:")
    await CreatePromocode.next()

@dp.message_handler(state=CreatePromocode.reward)
async def create_promo_reward(message: types.Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.finish()
        await admin_promo_menu(message)
        return
    try:
        reward = int(message.text)
        if reward <= 0:
            raise ValueError
    except:
        await message.answer("Введи положительное целое число.")
        return
    await state.update_data(reward=reward)
    await message.answer("Введи максимальное количество использований:")
    await CreatePromocode.next()

@dp.message_handler(state=CreatePromocode.max_uses)
async def create_promo_max_uses(message: types.Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.finish()
        await admin_promo_menu(message)
        return
    try:
        max_uses = int(message.text)
        if max_uses <= 0:
            raise ValueError
    except:
        await message.answer("Введи положительное целое число.")
        return
    data = await state.get_data()
    try:
        async with aiosqlite.connect(DB_PATH, timeout=10) as db:
            await db.execute(
                "INSERT INTO promocodes (code, reward, max_uses) VALUES (?, ?, ?)",
                (data['code'], data['reward'], max_uses)
            )
            await db.commit()
        await message.answer("✅ Промокод создан!", reply_markup=promo_admin_keyboard())
    except aiosqlite.IntegrityError:
        await message.answer("❌ Промокод с таким кодом уже существует.")
    except Exception as e:
        logging.error(f"Create promo error: {e}")
        await message.answer("❌ Ошибка.")
    await state.finish()

@dp.message_handler(lambda message: message.text == "📋 Список промокодов")
async def list_promos(message: types.Message):
    if not await is_admin(message.from_user.id):
        return
    try:
        async with aiosqlite.connect(DB_PATH, timeout=10) as db:
            async with db.execute("SELECT code, reward, max_uses, used_count FROM promocodes") as cur:
                rows = await cur.fetchall()
        if not rows:
            await message.answer("Нет промокодов.")
            return
        text = "🎫 Промокоды:\n"
        for code, reward, max_uses, used in rows:
            text += f"• {code}: {reward} монет, использовано {used}/{max_uses}\n"
        await message.answer(text, reply_markup=promo_admin_keyboard())
    except Exception as e:
        logging.error(f"List promos error: {e}")
        await message.answer("❌ Ошибка.")

# ===== НАСТРОЙКИ ИГРЫ =====
@dp.message_handler(lambda message: message.text == "⚙️ Настройки игры")
async def settings_menu(message: types.Message):
    if not await is_admin(message.from_user.id):
        return
    settings = {}
    async with aiosqlite.connect(DB_PATH, timeout=10) as db:
        async with db.execute("SELECT key, value FROM settings") as cur:
            rows = await cur.fetchall()
            for key, value in rows:
                settings[key] = value
    text = "⚙️ <b>Текущие настройки игры:</b>\n\n"
    text += f"💰 Стоимость случайной кражи: {settings.get('random_attack_cost', '0')} монет\n"
    text += f"👤 Стоимость кражи по username: {settings.get('targeted_attack_cost', '50')} монет\n"
    text += f"⏱ Кулдаун между кражами: {settings.get('theft_cooldown_minutes', '30')} мин\n"
    text += f"🎲 Шанс успеха кражи: {settings.get('theft_success_chance', '40')}%\n"
    text += f"🛡 Шанс защиты жертвы: {settings.get('theft_defense_chance', '20')}%\n"
    text += f"💥 Штраф при защите: {settings.get('theft_defense_penalty', '10')} монет\n"
    text += f"🎰 Шанс выигрыша в казино: {settings.get('casino_win_chance', '30')}%\n"
    text += f"💰 Мин. сумма кражи: {settings.get('min_theft_amount', '5')}\n"
    text += f"💰 Макс. сумма кражи: {settings.get('max_theft_amount', '15')}\n\n"
    text += "Выбери параметр для изменения:"
    await message.answer(text, reply_markup=settings_keyboard())

@dp.callback_query_handler(lambda c: c.data.startswith("set_"))
async def set_setting_callback(callback: types.CallbackQuery, state: FSMContext):
    if not await is_admin(callback.from_user.id):
        await callback.answer("Нет прав", show_alert=True)
        return
    key_map = {
        "set_random_cost": "random_attack_cost",
        "set_targeted_cost": "targeted_attack_cost",
        "set_cooldown": "theft_cooldown_minutes",
        "set_success_chance": "theft_success_chance",
        "set_defense_chance": "theft_defense_chance",
        "set_defense_penalty": "theft_defense_penalty",
        "set_casino_chance": "casino_win_chance",
        "set_min_theft": "min_theft_amount",
        "set_max_theft": "max_theft_amount",
    }
    key = key_map.get(callback.data)
    if not key:
        await callback.answer("Неизвестный параметр", show_alert=True)
        return
    await state.update_data(setting_key=key)
    await callback.message.edit_text(f"Введи новое значение для параметра (целое число):")
    await EditSettings.key.set()
    await callback.answer()

@dp.message_handler(state=EditSettings.key)
async def set_setting_value(message: types.Message, state: FSMContext):
    try:
        value = int(message.text)
    except:
        await message.answer("Введи целое число.")
        return
    data = await state.get_data()
    key = data['setting_key']
    await set_setting(key, str(value))
    await message.answer(f"✅ Параметр обновлён.")
    await state.finish()
    await settings_menu(message)

@dp.callback_query_handler(lambda c: c.data == "back_to_admin")
async def back_to_admin(callback: types.CallbackQuery):
    if not await is_admin(callback.from_user.id):
        return
    super_admin = await is_super_admin(callback.from_user.id)
    await callback.message.edit_text("Панель администратора:", reply_markup=admin_main_keyboard(super_admin))
    await callback.answer()

# ===== СТАТИСТИКА =====
@dp.message_handler(lambda message: message.text == "📊 Статистика")
async def stats_handler(message: types.Message):
    if not await is_admin(message.from_user.id):
        return
    try:
        async with aiosqlite.connect(DB_PATH, timeout=10) as db:
            async with db.execute("SELECT COUNT(*) FROM users") as cur:
                users = (await cur.fetchone())[0]
            async with db.execute("SELECT SUM(balance) FROM users") as cur:
                total_balance = (await cur.fetchone())[0] or 0
            async with db.execute("SELECT COUNT(*) FROM giveaways WHERE status='active'") as cur:
                active_giveaways = (await cur.fetchone())[0]
            async with db.execute("SELECT COUNT(*) FROM shop_items") as cur:
                shop_items = (await cur.fetchone())[0]
            async with db.execute("SELECT COUNT(*) FROM purchases WHERE status='pending'") as cur:
                purchases_pending = (await cur.fetchone())[0]
            async with db.execute("SELECT COUNT(*) FROM purchases WHERE status='completed'") as cur:
                purchases_completed = (await cur.fetchone())[0]
            async with db.execute("SELECT SUM(theft_attempts) FROM users") as cur:
                total_thefts = (await cur.fetchone())[0] or 0
            async with db.execute("SELECT SUM(theft_success) FROM users") as cur:
                total_thefts_success = (await cur.fetchone())[0] or 0
            async with db.execute("SELECT COUNT(*) FROM promocodes") as cur:
                promos = (await cur.fetchone())[0]
            async with db.execute("SELECT COUNT(*) FROM banned_users") as cur:
                banned = (await cur.fetchone())[0]
        text = (
            f"📊 Статистика:\n"
            f"👥 Пользователей: {users}\n"
            f"💰 Всего монет: {total_balance}\n"
            f"🎁 Активных розыгрышей: {active_giveaways}\n"
            f"🛒 Товаров в магазине: {shop_items}\n"
            f"🛍️ Ожидающих покупок: {purchases_pending}\n"
            f"✅ Выполненных покупок: {purchases_completed}\n"
            f"🔫 Всего ограблений: {total_thefts} (успешно: {total_thefts_success})\n"
            f"🎫 Промокодов создано: {promos}\n"
            f"⛔ Заблокировано: {banned}"
        )
        await message.answer(text, reply_markup=admin_main_keyboard(await is_super_admin(message.from_user.id)))
    except Exception as e:
        logging.error(f"Stats error: {e}")
        await message.answer("❌ Ошибка получения статистики.")

# ===== НАЙТИ ПОЛЬЗОВАТЕЛЯ =====
@dp.message_handler(lambda message: message.text == "👥 Найти пользователя")
async def find_user_start(message: types.Message):
    if not await is_admin(message.from_user.id):
        return
    await message.answer("Введи ID или @username пользователя:", reply_markup=back_keyboard())
    await FindUser.query.set()

@dp.message_handler(state=FindUser.query)
async def find_user_result(message: types.Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.finish()
        super_admin = await is_super_admin(message.from_user.id)
        await message.answer("Панель администратора:", reply_markup=admin_main_keyboard(super_admin))
        return
    query = message.text.strip()
    try:
        row = None
        try:
            uid = int(query)
            async with aiosqlite.connect(DB_PATH, timeout=10) as db:
                async with db.execute("SELECT user_id, first_name, balance, joined_date, theft_attempts, theft_success, theft_failed, theft_protected FROM users WHERE user_id=?", (uid,)) as cur:
                    row = await cur.fetchone()
        except ValueError:
            username = query.lower()
            if username.startswith('@'):
                username = username[1:]
            async with aiosqlite.connect(DB_PATH, timeout=10) as db:
                async with db.execute("SELECT user_id, first_name, balance, joined_date, theft_attempts, theft_success, theft_failed, theft_protected FROM users WHERE LOWER(username)=?", (username,)) as cur:
                    row = await cur.fetchone()
        if not row:
            await message.answer("❌ Пользователь не найден.")
            return
        uid, name, bal, joined, attempts, success, failed, protected = row
        banned = await is_banned(uid)
        ban_status = "⛔ Заблокирован" if banned else "✅ Активен"
        text = (
            f"👤 Пользователь: {name} (ID: {uid})\n"
            f"💰 Баланс: {bal}\n"
            f"📅 Регистрация: {joined}\n"
            f"🔫 Ограблений: {attempts} (успешно: {success}, провал: {failed})\n"
            f"⚔️ Отбито атак: {protected}\n"
            f"Статус: {ban_status}"
        )
        await message.answer(text)
    except Exception as e:
        logging.error(f"Find user error: {e}")
        await message.answer("❌ Ошибка поиска.")
    await state.finish()

# ===== СПИСОК ПОКУПОК (АДМИН) =====
@dp.message_handler(lambda message: message.text == "🛍️ Список покупок")
async def admin_purchases(message: types.Message):
    if not await is_admin(message.from_user.id):
        return
    try:
        async with aiosqlite.connect(DB_PATH, timeout=10) as db:
            async with db.execute(
                "SELECT p.id, u.user_id, u.username, s.name, p.purchase_date, p.status FROM purchases p JOIN users u ON p.user_id = u.user_id JOIN shop_items s ON p.item_id = s.id WHERE p.status='pending' ORDER BY p.purchase_date"
            ) as cursor:
                purchases = await cursor.fetchall()
        if not purchases:
            await message.answer("Нет необработанных покупок.")
            return
        for pid, uid, username, item_name, date, status in purchases:
            text = f"🆔 {pid}\nПользователь: {uid} (@{username})\nТовар: {item_name}\nДата: {date}"
            await message.answer(text, reply_markup=purchase_action_keyboard(pid))
    except Exception as e:
        logging.error(f"Admin purchases error: {e}")
        await message.answer("❌ Ошибка загрузки покупок.")

@dp.callback_query_handler(lambda c: c.data.startswith("purchase_done_"))
async def purchase_done(callback: types.CallbackQuery):
    if not await is_admin(callback.from_user.id):
        await callback.answer("Недостаточно прав", show_alert=True)
        return
    purchase_id = int(callback.data.split("_")[2])
    try:
        async with aiosqlite.connect(DB_PATH, timeout=10) as db:
            await db.execute("UPDATE purchases SET status='completed' WHERE id=?", (purchase_id,))
            await db.commit()
            async with db.execute("SELECT user_id FROM purchases WHERE id=?", (purchase_id,)) as cur:
                row = await cur.fetchone()
                if row:
                    user_id = row[0]
                    safe_send_message_task(user_id, "✅ Твоя покупка обработана! Админ выслал подарок.")
        await callback.answer("Покупка отмечена как выполненная")
        await callback.message.delete()
    except Exception as e:
        logging.error(f"Purchase done error: {e}")
        await callback.answer("Ошибка", show_alert=True)

@dp.callback_query_handler(lambda c: c.data.startswith("purchase_reject_"))
async def purchase_reject(callback: types.CallbackQuery):
    if not await is_admin(callback.from_user.id):
        await callback.answer("Недостаточно прав", show_alert=True)
        return
    purchase_id = int(callback.data.split("_")[2])
    try:
        async with aiosqlite.connect(DB_PATH, timeout=10) as db:
            await db.execute("UPDATE purchases SET status='rejected' WHERE id=?", (purchase_id,))
            await db.commit()
            async with db.execute("SELECT user_id FROM purchases WHERE id=?", (purchase_id,)) as cur:
                row = await cur.fetchone()
                if row:
                    user_id = row[0]
                    safe_send_message_task(user_id, "❌ К сожалению, твоя покупка не может быть выполнена. Свяжись с админом.")
        await callback.answer("Покупка отклонена")
        await callback.message.delete()
    except Exception as e:
        logging.error(f"Purchase reject error: {e}")
        await callback.answer("Ошибка", show_alert=True)

# ===== ДОБАВЛЕНИЕ МЛАДШЕГО АДМИНА =====
@dp.message_handler(lambda message: message.text == "➕ Добавить админа")
async def add_admin_start(message: types.Message):
    if not await is_super_admin(message.from_user.id):
        await message.answer("Только суперадмин может добавлять админов.")
        return
    await message.answer("Введи ID пользователя, которого хочешь сделать младшим админом:", reply_markup=back_keyboard())
    await AddJuniorAdmin.user_id.set()

@dp.message_handler(state=AddJuniorAdmin.user_id)
async def add_admin_finish(message: types.Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.finish()
        super_admin = await is_super_admin(message.from_user.id)
        await message.answer("Панель администратора:", reply_markup=admin_main_keyboard(super_admin))
        return
    try:
        uid = int(message.text)
    except:
        await message.answer("❌ Введи числовой ID.")
        return
    try:
        async with aiosqlite.connect(DB_PATH, timeout=10) as db:
            async with db.execute("SELECT user_id FROM users WHERE user_id=?", (uid,)) as cur:
                if not await cur.fetchone():
                    await message.answer("❌ Пользователь с таким ID не найден в боте.")
                    return
            await db.execute("INSERT INTO admins (user_id, added_by, added_date) VALUES (?, ?, ?)",
                             (uid, message.from_user.id, datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
            await db.commit()
        await message.answer(f"✅ Пользователь {uid} теперь младший админ.")
    except aiosqlite.IntegrityError:
        await message.answer("❌ Этот пользователь уже админ.")
    except Exception as e:
        logging.error(f"Add admin error: {e}")
        await message.answer("❌ Ошибка.")
    await state.finish()

# ===== УДАЛЕНИЕ МЛАДШЕГО АДМИНА =====
@dp.message_handler(lambda message: message.text == "➖ Удалить админа")
async def remove_admin_start(message: types.Message):
    if not await is_super_admin(message.from_user.id):
        await message.answer("Только суперадмин может удалять админов.")
        return
    await message.answer("Введи ID пользователя, которого хочешь лишить прав админа:", reply_markup=back_keyboard())
    await RemoveJuniorAdmin.user_id.set()

@dp.message_handler(state=RemoveJuniorAdmin.user_id)
async def remove_admin_finish(message: types.Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.finish()
        super_admin = await is_super_admin(message.from_user.id)
        await message.answer("Панель администратора:", reply_markup=admin_main_keyboard(super_admin))
        return
    try:
        uid = int(message.text)
    except:
        await message.answer("❌ Введи числовой ID.")
        return
    try:
        async with aiosqlite.connect(DB_PATH, timeout=10) as db:
            await db.execute("DELETE FROM admins WHERE user_id=?", (uid,))
            await db.commit()
        await message.answer(f"✅ Пользователь {uid} больше не админ, если был им.")
    except Exception as e:
        logging.error(f"Remove admin error: {e}")
        await message.answer("❌ Ошибка.")
    await state.finish()

# ===== БЛОКИРОВКА ПОЛЬЗОВАТЕЛЯ =====
@dp.message_handler(lambda message: message.text == "🔨 Заблокировать пользователя")
async def block_user_start(message: types.Message):
    if not await is_admin(message.from_user.id):
        return
    await message.answer("Введи ID пользователя для блокировки:", reply_markup=back_keyboard())
    await BlockUser.user_id.set()

@dp.message_handler(state=BlockUser.user_id)
async def block_user_id(message: types.Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.finish()
        super_admin = await is_super_admin(message.from_user.id)
        await message.answer("Панель администратора:", reply_markup=admin_main_keyboard(super_admin))
        return
    try:
        uid = int(message.text)
    except:
        await message.answer("❌ Введи числовой ID.")
        return
    if await is_admin(uid):
        await message.answer("❌ Нельзя заблокировать администратора.")
        await state.finish()
        return
    await state.update_data(user_id=uid)
    await message.answer("Введи причину блокировки (можно отправить 'нет'):")
    await BlockUser.reason.set()

@dp.message_handler(state=BlockUser.reason)
async def block_user_reason(message: types.Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.finish()
        super_admin = await is_super_admin(message.from_user.id)
        await message.answer("Панель администратора:", reply_markup=admin_main_keyboard(super_admin))
        return
    reason = None if message.text.lower() == 'нет' else message.text
    data = await state.get_data()
    uid = data['user_id']
    try:
        async with aiosqlite.connect(DB_PATH, timeout=10) as db:
            await db.execute("INSERT OR IGNORE INTO banned_users (user_id, banned_by, banned_date, reason) VALUES (?, ?, ?, ?)",
                             (uid, message.from_user.id, datetime.now().strftime("%Y-%m-%d %H:%M:%S"), reason))
            await db.commit()
        await message.answer(f"✅ Пользователь {uid} заблокирован.")
        safe_send_message_task(uid, f"⛔ Вы заблокированы в боте. Причина: {reason if reason else 'не указана'}")
    except Exception as e:
        logging.error(f"Block user error: {e}")
        await message.answer("❌ Ошибка.")
    await state.finish()

# ===== РАЗБЛОКИРОВКА =====
@dp.message_handler(lambda message: message.text == "🔓 Разблокировать пользователя")
async def unblock_user_start(message: types.Message):
    if not await is_admin(message.from_user.id):
        return
    await message.answer("Введи ID пользователя для разблокировки:", reply_markup=back_keyboard())
    await UnblockUser.user_id.set()

@dp.message_handler(state=UnblockUser.user_id)
async def unblock_user_finish(message: types.Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.finish()
        super_admin = await is_super_admin(message.from_user.id)
        await message.answer("Панель администратора:", reply_markup=admin_main_keyboard(super_admin))
        return
    try:
        uid = int(message.text)
    except:
        await message.answer("❌ Введи числовой ID.")
        return
    try:
        async with aiosqlite.connect(DB_PATH, timeout=10) as db:
            await db.execute("DELETE FROM banned_users WHERE user_id=?", (uid,))
            await db.commit()
        await message.answer(f"✅ Пользователь {uid} разблокирован.")
        safe_send_message_task(uid, "🔓 Вы разблокированы в боте.")
    except Exception as e:
        logging.error(f"Unblock user error: {e}")
        await message.answer("❌ Ошибка.")
    await state.finish()

# ===== СПИСАНИЕ МОНЕТ =====
@dp.message_handler(lambda message: message.text == "💸 Списать монеты")
async def remove_balance_start(message: types.Message):
    if not await is_admin(message.from_user.id):
        return
    await message.answer("Введи ID пользователя:", reply_markup=back_keyboard())
    await RemoveBalance.user_id.set()

@dp.message_handler(state=RemoveBalance.user_id)
async def remove_balance_user(message: types.Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.finish()
        super_admin = await is_super_admin(message.from_user.id)
        await message.answer("Панель администратора:", reply_markup=admin_main_keyboard(super_admin))
        return
    try:
        uid = int(message.text)
    except:
        await message.answer("❌ Введи число.")
        return
    await state.update_data(user_id=uid)
    await message.answer("Введи сумму списания (целое положительное число):")
    await RemoveBalance.amount.set()

@dp.message_handler(state=RemoveBalance.amount)
async def remove_balance_amount(message: types.Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.finish()
        super_admin = await is_super_admin(message.from_user.id)
        await message.answer("Панель администратора:", reply_markup=admin_main_keyboard(super_admin))
        return
    try:
        amount = int(message.text)
        if amount <= 0:
            raise ValueError
    except:
        await message.answer("Введи положительное целое число.")
        return
    data = await state.get_data()
    uid = data['user_id']
    try:
        async with aiosqlite.connect(DB_PATH, timeout=10) as db:
            await db.execute("UPDATE users SET balance = balance - ? WHERE user_id=?", (amount, uid))
            await db.commit()
        await message.answer(f"✅ У пользователя {uid} списано {amount} монет.")
        safe_send_message_task(uid, f"💸 У тебя списано {amount} монет администратором.")
    except Exception as e:
        logging.error(f"Remove balance error: {e}")
        await message.answer("❌ Ошибка.")
    await state.finish()

# ===== НАЧИСЛЕНИЕ МОНЕТ =====
@dp.message_handler(lambda message: message.text == "💰 Начислить монеты")
async def add_balance_start(message: types.Message):
    if not await is_admin(message.from_user.id):
        return
    await message.answer("Введи ID пользователя:", reply_markup=back_keyboard())
    await AddBalance.user_id.set()

@dp.message_handler(state=AddBalance.user_id)
async def add_balance_user(message: types.Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.finish()
        super_admin = await is_super_admin(message.from_user.id)
        await message.answer("Панель администратора:", reply_markup=admin_main_keyboard(super_admin))
        return
    try:
        uid = int(message.text)
    except:
        await message.answer("❌ Введи число.")
        return
    await state.update_data(user_id=uid)
    await message.answer("Введи сумму начисления (целое положительное число):")
    await AddBalance.amount.set()

@dp.message_handler(state=AddBalance.amount)
async def add_balance_amount(message: types.Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.finish()
        super_admin = await is_super_admin(message.from_user.id)
        await message.answer("Панель администратора:", reply_markup=admin_main_keyboard(super_admin))
        return
    try:
        amount = int(message.text)
        if amount <= 0:
            raise ValueError
    except:
        await message.answer("Введи положительное целое число.")
        return
    data = await state.get_data()
    uid = data['user_id']
    try:
        async with aiosqlite.connect(DB_PATH, timeout=10) as db:
            await db.execute("UPDATE users SET balance = balance + ? WHERE user_id=?", (amount, uid))
            await db.commit()
        await message.answer(f"✅ Пользователю {uid} начислено {amount} монет.")
        safe_send_message_task(uid, f"💰 Вам начислено {amount} монет администратором.")
    except Exception as e:
        logging.error(f"Add balance error: {e}")
        await message.answer("❌ Ошибка.")
    await state.finish()

# ===== СБРОС СТАТИСТИКИ =====
@dp.message_handler(lambda message: message.text == "🔄 Сброс статистики")
async def reset_stats(message: types.Message):
    if not await is_super_admin(message.from_user.id):
        return
    confirm_kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Да, сбросить всё", callback_data="reset_confirm")],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="reset_cancel")]
    ])
    await message.answer("⚠️ Ты уверен? Это действие безвозвратно обнулит балансы, покупки и статистику всех пользователей.", reply_markup=confirm_kb)

@dp.callback_query_handler(lambda c: c.data == "reset_confirm")
async def reset_confirm(callback: types.CallbackQuery):
    if not await is_super_admin(callback.from_user.id):
        return
    try:
        async with aiosqlite.connect(DB_PATH, timeout=10) as db:
            await db.execute("UPDATE users SET balance=0, theft_attempts=0, theft_success=0, theft_failed=0, theft_protected=0, last_theft_time=NULL")
            await db.execute("DELETE FROM purchases")
            await db.commit()
        await callback.message.edit_text("✅ Статистика сброшена.")
    except Exception as e:
        logging.error(f"Reset error: {e}")
        await callback.message.edit_text("❌ Ошибка при сбросе.")
    await callback.answer()

@dp.callback_query_handler(lambda c: c.data == "reset_cancel")
async def reset_cancel(callback: types.CallbackQuery):
    await callback.message.edit_text("Сброс отменён.")
    await callback.answer()

# ===== РАССЫЛКА =====
@dp.message_handler(lambda message: message.text == "📢 Рассылка")
async def broadcast_start(message: types.Message):
    if not await is_admin(message.from_user.id):
        return
    await message.answer("Отправь сообщение для рассылки (текст, фото, видео или документ).", reply_markup=back_keyboard())
    await Broadcast.media.set()

@dp.message_handler(state=Broadcast.media, content_types=['text', 'photo', 'video', 'document'])
async def broadcast_media(message: types.Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.finish()
        super_admin = await is_super_admin(message.from_user.id)
        await message.answer("Панель администратора:", reply_markup=admin_main_keyboard(super_admin))
        return

    content = {}
    if message.text:
        content['type'] = 'text'
        content['text'] = message.text
    elif message.photo:
        content['type'] = 'photo'
        content['file_id'] = message.photo[-1].file_id
        content['caption'] = message.caption or ""
    elif message.video:
        content['type'] = 'video'
        content['file_id'] = message.video.file_id
        content['caption'] = message.caption or ""
    elif message.document:
        content['type'] = 'document'
        content['file_id'] = message.document.file_id
        content['caption'] = message.caption or ""
    else:
        await message.answer("Неподдерживаемый тип.")
        return

    await state.finish()

    status_msg = await message.answer("⏳ Рассылка начата... Это может занять некоторое время.")

    async with aiosqlite.connect(DB_PATH, timeout=10) as db:
        async with db.execute("SELECT user_id FROM users") as cur:
            users = [row[0] for row in await cur.fetchall()]

    sent = 0
    failed = 0
    total = len(users)

    for i, uid in enumerate(users):
        if await is_banned(uid):
            continue
        try:
            if content['type'] == 'text':
                await bot.send_message(uid, content['text'])
            elif content['type'] == 'photo':
                await bot.send_photo(uid, content['file_id'], caption=content['caption'])
            elif content['type'] == 'video':
                await bot.send_video(uid, content['file_id'], caption=content['caption'])
            elif content['type'] == 'document':
                await bot.send_document(uid, content['file_id'], caption=content['caption'])
            sent += 1
        except (BotBlocked, UserDeactivated, ChatNotFound):
            failed += 1
        except RetryAfter as e:
            logging.warning(f"Flood limit, waiting {e.timeout} seconds")
            await asyncio.sleep(e.timeout)
            try:
                if content['type'] == 'text':
                    await bot.send_message(uid, content['text'])
                else:
                    if content['type'] == 'photo':
                        await bot.send_photo(uid, content['file_id'], caption=content['caption'])
                    elif content['type'] == 'video':
                        await bot.send_video(uid, content['file_id'], caption=content['caption'])
                    elif content['type'] == 'document':
                        await bot.send_document(uid, content['file_id'], caption=content['caption'])
                sent += 1
            except:
                failed += 1
        except Exception as e:
            failed += 1
            logging.warning(f"Failed to send to {uid}: {e}")

        if (i + 1) % 10 == 0:
            try:
                await status_msg.edit_text(f"⏳ Прогресс: {i+1}/{total}\n✅ Отправлено: {sent}\n❌ Ошибок: {failed}")
            except:
                pass

        await asyncio.sleep(0.05)

    await status_msg.edit_text(f"✅ Рассылка завершена!\n📊 Отправлено: {sent}\n❌ Ошибок: {failed}\n👥 Всего: {total}")

# ===== НАЗАД В ГЛАВНОЕ МЕНЮ =====
@dp.message_handler(lambda message: message.text == "◀️ Назад в главное меню")
async def back_to_main_from_admin(message: types.Message):
    admin_flag = await is_admin(message.from_user.id)
    await message.answer("Главное меню:", reply_markup=user_main_keyboard(admin_flag))

@dp.message_handler(lambda message: message.text == "◀️ Назад")
async def back_from_submenu(message: types.Message):
    admin_flag = await is_admin(message.from_user.id)
    await message.answer("Главное меню:", reply_markup=user_main_keyboard(admin_flag))

# ===== ОБРАБОТКА НЕИЗВЕСТНЫХ СООБЩЕНИЙ =====
@dp.message_handler()
async def unknown_message(message: types.Message):
    if await is_banned(message.from_user.id) and not await is_admin(message.from_user.id):
        return
    admin_flag = await is_admin(message.from_user.id)
    await message.answer("Я не понимаю эту команду. Используй кнопки меню.", reply_markup=user_main_keyboard(admin_flag))

# ===== ВЕБ-СЕРВЕР ДЛЯ RAILWAY =====
async def handle(request):
    return web.Response(text="Bot is running")

async def start_web_server():
    app = web.Application()
    app.router.add_get("/", handle)
    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.environ.get("PORT", 8080))
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    logging.info(f"Web server started on port {port}")

# ===== ФОНОВЫЕ ЗАДАЧИ =====
async def check_expired_giveaways():
    while True:
        await asyncio.sleep(600)
        try:
            async with aiosqlite.connect(DB_PATH, timeout=10) as db:
                now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                await db.execute("UPDATE giveaways SET status='completed' WHERE status='active' AND end_date < ?", (now,))
                await db.commit()
        except Exception as e:
            logging.error(f"Expired giveaways check error: {e}")

# ===== ЗАПУСК =====
async def on_startup(dp):
    await before_start()  # удаляем вебхук и пропускаем старые обновления
    await init_db()
    asyncio.create_task(check_expired_giveaways())
    asyncio.create_task(start_web_server())
    logging.info("🤖 Бот запущен и готов к работе!")
    logging.info(f"👑 Суперадмины: {SUPER_ADMINS}")
    logging.info(f"🗄 База данных: {DB_PATH}")

async def on_shutdown(dp):
    await storage.close()
    await dp.storage.close()
    await bot.close()
    logging.info("Бот остановлен")

if __name__ == "__main__":
    # Запуск с обработкой ошибки TerminatedByOtherGetUpdates
    while True:
        try:
            executor.start_polling(dp, skip_updates=True, on_startup=on_startup, on_shutdown=on_shutdown)
        except TerminatedByOtherGetUpdates:
            logging.error("Обнаружен конфликт с другим экземпляром бота. Жду 5 секунд и перезапускаю...")
            time.sleep(5)
            continue
        except Exception as e:
            error_name = type(e).__name__
            if "TerminatedByOtherGetUpdate" in error_name or "TerminatedByOtherGetUpdates" in error_name:
                logging.error("Обнаружен конфликт с другим экземпляром бота. Повторная попытка через 5 секунд...")
            else:
                logging.error(f"Критическая ошибка: {e}")
            time.sleep(5)
            continue
