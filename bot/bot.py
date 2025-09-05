import io
import os
import logging
import json
from pathlib import Path

import httpx
from aiogram import Bot, Dispatcher, executor, types
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters.state import State, StatesGroup


BOT_TOKEN = os.getenv("BOT_TOKEN", "")
API_BASE_URL = os.getenv("API_BASE_URL", "http://api:8000")

logging.basicConfig(level=logging.INFO)

bot = Bot(token=BOT_TOKEN)
dispatcher = Dispatcher(bot, storage=MemoryStorage())


# --- HTTP helpers ---
async def api_post(path, json):
    async with httpx.AsyncClient(timeout=10.0) as client:
        r = await client.post(API_BASE_URL + path, json=json)
        r.raise_for_status()
        return r.json()


async def api_get(path):
    async with httpx.AsyncClient(timeout=10.0) as client:
        r = await client.get(API_BASE_URL + path)
        r.raise_for_status()
        return r.json()


# states
class RegisterStates(StatesGroup):
    waiting_team = State()
    waiting_endpoint = State()


# --- Keyboards ---
def kb_unregistered() -> types.InlineKeyboardMarkup:
    kb = types.InlineKeyboardMarkup(row_width=1)
    kb.add(types.InlineKeyboardButton(text="Регистрация команды", callback_data="register"))
    return kb


def kb_registered() -> types.InlineKeyboardMarkup:
    kb = types.InlineKeyboardMarkup(row_width=1)
    kb.add(
        types.InlineKeyboardButton(text="Проверить решение", callback_data="run"),
        types.InlineKeyboardButton(text="Показать результаты последней отправки", callback_data="last_result"),
        types.InlineKeyboardButton(text="Скачать датасет", callback_data="download_dataset"),
        types.InlineKeyboardButton(text="Лидерборд", callback_data="leaderboard"),
    )
    return kb


async def main_menu_keyboard(chat_id: int) -> types.InlineKeyboardMarkup:
    try:
        _ = await api_get(f"/teams/{chat_id}")
        is_registered = True
    except Exception:
        is_registered = False
    return kb_registered() if is_registered else kb_unregistered()


def _normalize_endpoint(s: str) -> str:
    s = s.strip()
    if not (s.startswith("http://") or s.startswith("https://")):
        s = "http://" + s
    if not s.endswith("/api/predict"):
        s = s.rstrip("/") + "/api/predict"
    return s


# --- /start ---
@dispatcher.message_handler(commands=["start", "help"])
async def cmd_start(message: types.Message):
    cid = message.chat.id
    try:
        team = await api_get(f"/teams/{cid}")
        is_registered = True
    except Exception:
        is_registered = False
        team = {}
    if is_registered:
        text = f"Готово! Команда: {team.get('name')}.\nВыберите действие:"
    else:
        text = "Добро пожаловать! Сначала зарегистрируйте команду."
    await message.reply(text, reply_markup=await main_menu_keyboard(cid))


# --- Callbacks: registration flow (2 steps) ---
@dispatcher.callback_query_handler(lambda c: c.data == "register")
async def cb_register(callback_query: types.CallbackQuery):
    await callback_query.answer()
    await bot.send_message(callback_query.message.chat.id, "Введите название команды:")
    await RegisterStates.waiting_team.set()


@dispatcher.message_handler(state=RegisterStates.waiting_team)
async def st_register_team(message: types.Message, state: FSMContext):
    team = message.text.strip()
    if not team:
        return await message.reply("Название команды не может быть пустым. Введите ещё раз:")
    await state.update_data(team_name=team)
    await message.reply("Теперь введите IP или URL вашего сервиса (например, 1.2.3.4:8000 или https://host):")
    await RegisterStates.waiting_endpoint.set()


@dispatcher.message_handler(state=RegisterStates.waiting_endpoint)
async def st_register_endpoint(message: types.Message, state: FSMContext):
    endpoint = _normalize_endpoint(message.text)
    data = await state.get_data()
    team_name = data.get("team_name")
    try:
        resp = await api_post("/teams/register", {"tg_chat_id": message.chat.id, "team_name": team_name, "endpoint_url": endpoint})
        await message.reply(
            f"Регистрация завершена: team_id={resp['team_id']}.",
            reply_markup=kb_registered()
        )
    except Exception:
        await message.reply("Ошибка регистрации", reply_markup=kb_unregistered())
    finally:
        await state.finish()


# --- Callbacks: run check and last result ---
@dispatcher.callback_query_handler(lambda c: c.data == "run")
async def cb_run(callback_query: types.CallbackQuery):
    cid = callback_query.message.chat.id
    await callback_query.answer()
    try:
        _ = await api_get(f"/teams/{cid}")
        is_registered = True
    except Exception:
        is_registered = False
    if not is_registered:
        return await bot.send_message(cid, "Сначала зарегистрируйте команду.", reply_markup=kb_unregistered())
    try:
        data = await api_post("/runs/start", {"tg_chat_id": cid})
        await bot.send_message(cid, f"Запущен тест: run_id={data['run_id']}, status={data['status']}", reply_markup=kb_registered())
    except Exception:
        await bot.send_message(cid, "Ошибка запуска", reply_markup=kb_registered())


@dispatcher.callback_query_handler(lambda c: c.data == "last_result")
async def cb_last_result(callback_query: types.CallbackQuery):
    cid = callback_query.message.chat.id
    await callback_query.answer()
    try:
        data = await api_get(f"/teams/{cid}/last_run")
        text = (
            f"Последний прогон run_id={data['run_id']}: {data['status']}\n"
            f"{data['samples_done']}/{data['samples_total']}\n"
            f"F1={data.get('f1_micro')} P={data.get('precision')} R={data.get('recall')}\n"
            f"avg_latency_ms={data.get('avg_latency_ms')}"
        )
        await bot.send_message(cid, text, reply_markup=kb_registered())
    except Exception:
        await bot.send_message(cid, "Ошибка получения статуса", reply_markup=kb_registered())


@dispatcher.callback_query_handler(lambda c: c.data == "download_dataset")
async def cb_download_dataset(callback_query: types.CallbackQuery):
    cid = callback_query.message.chat.id
    await callback_query.answer()
    try:
        async with httpx.AsyncClient(timeout=20.0) as client:
            r = await client.get(API_BASE_URL + "/phases/current/dataset", params={"tg_chat_id": cid})
            r.raise_for_status()
            data = r.content
        await bot.send_document(
            cid,
            types.InputFile(io.BytesIO(data), filename="dataset.csv"),
            caption=f"Готов для скачивания",
            reply_markup=kb_registered(),
        )
    except Exception:
        await bot.send_message(cid, "Ошибка загрузки датасета", reply_markup=kb_registered())


@dispatcher.callback_query_handler(lambda c: c.data == "leaderboard")
async def cb_leaderboard(callback_query: types.CallbackQuery):
    cid = callback_query.message.chat.id
    await callback_query.answer()
    try:
        data = await api_get("/leaderboard")
        items = data.get("items", [])
        if not items:
            text = "Лидерборд пока пуст"
        else:
            lines = []
            lines.append(f"{'#':>2}  {'Команда':<20}  {'F1':>6}  {'Latency, ms':>12}")
            lines.append("-" * 46)
            for idx, it in enumerate(items, start=1):
                name = str(it.get('team_name', ''))[:20]
                f1 = it.get('f1', 0.0) or 0.0
                lat = it.get('avg_latency_ms', 0.0) or 0.0
                lines.append(f"{idx:>2}.  {name:<20}  {f1:>6.4f}  {lat:>12.1f}")
            text = "```\n" + "\n".join(lines) + "\n```"
        await bot.send_message(cid, text, reply_markup=kb_registered(), parse_mode="Markdown")
    except Exception:
        await bot.send_message(cid, "Ошибка получения лидерборда", reply_markup=kb_registered())


if __name__ == "__main__":
    executor.start_polling(dispatcher, skip_updates=True)
