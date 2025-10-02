import os
import html
import logging
import asyncio

import httpx
from aiogram import Bot, Dispatcher, executor, types
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.handler import CancelHandler
from aiogram.dispatcher.middlewares import BaseMiddleware
from aiogram.dispatcher.filters.state import State, StatesGroup


BOT_TOKEN = os.getenv("BOT_TOKEN", "")
API_BASE_URL = os.getenv("API_BASE_URL", "http://api:8000")

logging.basicConfig(level=logging.INFO)

bot = Bot(token=BOT_TOKEN)
dispatcher = Dispatcher(bot, storage=MemoryStorage())

PROGRESS_WATCHERS: dict[int, asyncio.Task] = {}


class GroupOnlyMiddleware(BaseMiddleware):
    """Блокируем личные чаты: отвечаем подсказкой и останавливаем обработку."""

    async def on_process_message(self, message: types.Message, data: dict):
        try:
            if message.chat.type == types.ChatType.PRIVATE:
                await message.reply("Добавьте бота в чат команды")
                raise CancelHandler()
        except AttributeError:
            pass

    async def on_process_callback_query(self, callback_query: types.CallbackQuery, data: dict):
        try:
            chat = callback_query.message.chat if callback_query.message else None
            if chat and chat.type == types.ChatType.PRIVATE:
                # Закрываем лоадер у кнопки и шлём подсказку
                try:
                    await callback_query.answer()
                except Exception:
                    pass
                await bot.send_message(chat.id, "Добавьте бота в чат команды")
                raise CancelHandler()
        except AttributeError:
            pass

# Регистрируем middleware, чтобы хэндлеры работали только в группах
dispatcher.middleware.setup(GroupOnlyMiddleware())


class BackendError(Exception):
    def __init__(self, message: str, status: int | None = None):
        super().__init__(message)
        self.message = message
        self.status = status


def _extract_backend_error(resp: httpx.Response) -> str:
    try:
        data = resp.json()
    except Exception:
        text = (resp.text or "").strip()
        return f"Ошибка {resp.status_code}: {text or 'Неизвестная ошибка'}"

    if isinstance(data, dict) and "detail" in data:
        detail = data["detail"]
        if isinstance(detail, str):
            return detail
        if isinstance(detail, list):
            parts = []
            for item in detail:
                try:
                    msg = item.get("msg") if isinstance(item, dict) else None
                    loc = item.get("loc") if isinstance(item, dict) else None
                except Exception:
                    msg, loc = None, None
                if loc and msg:
                    parts.append(f"{'.'.join(str(p) for p in loc)}: {msg}")
                elif msg:
                    parts.append(str(msg))
                else:
                    parts.append(str(item))
            return "; ".join(parts) or f"Ошибка {resp.status_code}"
        return str(detail)

    if isinstance(data, list):
        parts = []
        for item in data:
            if isinstance(item, dict) and "msg" in item:
                loc = item.get("loc")
                if loc:
                    parts.append(f"{'.'.join(str(p) for p in loc)}: {item['msg']}")
                else:
                    parts.append(str(item["msg"]))
            else:
                parts.append(str(item))
        return "; ".join(parts) or f"Ошибка {resp.status_code}"

    return f"Ошибка {resp.status_code}: {data}"


async def api_post(path, json):
    async with httpx.AsyncClient(timeout=10.0) as client:
        try:
            r = await client.post(API_BASE_URL + path, json=json)
            r.raise_for_status()
            return r.json()
        except httpx.HTTPStatusError as e:
            raise BackendError(_extract_backend_error(e.response), e.response.status_code)
        except httpx.RequestError:
            raise BackendError("Сервис API недоступен. Проверьте URL и доступность.")


async def api_get(path):
    async with httpx.AsyncClient(timeout=10.0) as client:
        try:
            r = await client.get(API_BASE_URL + path)
            r.raise_for_status()
            return r.json()
        except httpx.HTTPStatusError as e:
            raise BackendError(_extract_backend_error(e.response), e.response.status_code)
        except httpx.RequestError:
            raise BackendError("Сервис API недоступен. Проверьте URL и доступность.")


async def api_post_multipart(path, data: dict, files: dict):
    async with httpx.AsyncClient(timeout=60.0) as client:
        try:
            r = await client.post(API_BASE_URL + path, data=data, files=files)
            r.raise_for_status()
            return r.json()
        except httpx.HTTPStatusError as e:
            raise BackendError(_extract_backend_error(e.response), e.response.status_code)
        except httpx.RequestError:
            raise BackendError("Сервис API недоступен. Проверьте URL и доступность.")


class RegisterStates(StatesGroup):
    """Регистрация команды"""
    waiting_team = State()


def kb_unregistered() -> types.InlineKeyboardMarkup:
    kb = types.InlineKeyboardMarkup(row_width=1)
    kb.add(types.InlineKeyboardButton(text="📝 Регистрация команды", callback_data="register"))
    return kb


def kb_registered() -> types.InlineKeyboardMarkup:
    kb = types.InlineKeyboardMarkup(row_width=2)
    btn_lb = types.InlineKeyboardButton(text="🏆 Лидерборд public", callback_data="leaderboard")
    btn_final_lb = types.InlineKeyboardButton(text="🏆 Лидерборд private", callback_data="final_leaderboard")
    kb.row(btn_final_lb)
    kb.row(btn_lb)
    return kb


def kb_cancel_inline() -> types.InlineKeyboardMarkup:
    kb = types.InlineKeyboardMarkup(row_width=1)
    kb.add(types.InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_flow"))
    return kb


async def main_menu_keyboard(chat_id: int) -> types.InlineKeyboardMarkup:
    try:
        _ = await api_get(f"/teams/{chat_id}")
        is_registered = True
    except BackendError as e:
        is_registered = False if e.status == 404 else True
    except Exception:
        is_registered = False
    return kb_registered() if is_registered else kb_unregistered()


@dispatcher.message_handler(commands=["start", "help"], state='*')
async def cmd_start(message: types.Message, state: FSMContext):
    cid = message.chat.id
    # Всегда выходим из любого активного состояния при /start
    try:
        await state.finish()
    except Exception:
        pass
    try:
        team = await api_get(f"/teams/{cid}")
        name = team.get('name') or '—'
        tg_username = team.get('tg_username') or '—'
        url = team.get('endpoint_url') or '—'
        gh = team.get('github_url') or '—'
        def _mark(v: str) -> str:
            try:
                return "✅" if (v and str(v).strip() and str(v).strip() != '—') else "❗"
            except Exception:
                return "❗"
        text = (
            f"👥 <b>Команда</b> {_mark(name)}:\n"
            f"-- {html.escape(str(name))}\n\n"
            f"👤 <b>Контактный Telegram username</b> {_mark(tg_username)}:\n"
            f"-- {html.escape(str(tg_username))}\n\n"
            f"🔗 <b>Текущий URL</b> {_mark(url)}:\n"
            f"-- {html.escape(str(url))}\n\n"
            f"📦 <b>Текущий GitHub</b> {_mark(gh)}:\n"
            f"-- {html.escape(str(gh))}\n\n"
        )
        kb = kb_registered()
    except BackendError as e:
        if e.status == 404:
            text = "Добро пожаловать! Сначала зарегистрируйте команду."
            kb = kb_unregistered()
        else:
            text = f"Не удалось проверить регистрацию: {e.message}"
            kb = kb_unregistered()
    except Exception:
        text = "Не удалось проверить регистрацию (неожиданная ошибка)."
        kb = kb_unregistered()
    await message.reply(text, reply_markup=kb, parse_mode="HTML")


@dispatcher.callback_query_handler(lambda c: c.data == "register", state='*')
async def cb_register(callback_query: types.CallbackQuery, state: FSMContext):
    await callback_query.answer()
    # Закрываем любой предыдущий flow перед началом регистрации
    try:
        await state.finish()
    except Exception:
        pass
    await bot.send_message(callback_query.message.chat.id, "Введите название команды:", reply_markup=kb_cancel_inline())
    await RegisterStates.waiting_team.set()


@dispatcher.message_handler(state=RegisterStates.waiting_team)
async def st_register_team(message: types.Message, state: FSMContext):
    if not message.text or not isinstance(message.text, str):
        return await message.reply("Пожалуйста, отправьте название команды текстом. Или /cancel для отмены.")
    if message.text.startswith('/'):
        return await message.reply("Это похоже на команду. Отправьте название команды текстом или используйте /cancel.")
    team = message.text.strip()
    if not team:
        return await message.reply("Название команды не может быть пустым. Введите ещё раз:")
    # Регистрируем команду без ожидания URL сервиса
    tg_username = (message.from_user.username or "").strip()
    if not tg_username:
        # Фолбэк, если у пользователя не задан username в Telegram
        tg_username = f"id_{message.from_user.id}"
    try:
        resp = await api_post(
            "/teams/register",
            {
                "tg_chat_id": message.chat.id,
                "team_name": team,
                "tg_username": tg_username,
            },
        )
        name = resp.get('name', team)
        await message.reply(
            (
                f"Регистрация завершена.\n"
                f"Название команды: {html.escape(str(name))}\n\n"
            ),
            reply_markup=kb_registered(),
            parse_mode="HTML",
        )
        await state.finish()
    except BackendError as e:
        await message.reply(f"Ошибка регистрации: {e.message}", reply_markup=kb_unregistered())
        await state.finish()
    except Exception:
        await message.reply("Неожиданная ошибка при регистрации", reply_markup=kb_unregistered())
        await state.finish()


@dispatcher.callback_query_handler(lambda c: c.data == "leaderboard", state='*')
async def cb_leaderboard(callback_query: types.CallbackQuery):
    cid = callback_query.message.chat.id
    await callback_query.answer()
    try:
        data = await api_get("/leaderboard?phase_id=34")
        items = data.get("items", [])
        header = f"🏆 Лидерборд (public)"
        if not items:
            text = f"{header}\nЛидерборд пока пуст"
        else:
            lines = []
            lines.append(f"{'#':>2}  {'Команда':<20}  {'F1':>6}  {'Latency, ms':>12}")
            lines.append("-" * 46)
            for idx, it in enumerate(items, start=1):
                name = str(it.get('team_name', ''))[:20]
                f1_val = it.get('f1', None)
                lat_val = it.get('avg_latency_ms', None)
                f1_str = '-' if f1_val is None else f"{float(f1_val):.4f}"
                lat_str = '-' if lat_val is None else f"{float(lat_val):.1f}"
                lines.append(f"{idx:>2}.  {name:<20}  {f1_str:>6}  {lat_str:>12}")
            text = header + "\n" + "```\n" + "\n".join(lines) + "\n```"
        await bot.send_message(cid, text, reply_markup=kb_registered(), parse_mode="Markdown")
    except BackendError as e:
        await bot.send_message(cid, f"Ошибка получения лидерборда: {e.message}", reply_markup=kb_registered())
    except Exception:
        await bot.send_message(cid, "Неожиданная ошибка при получении лидерборда", reply_markup=kb_registered())


@dispatcher.callback_query_handler(lambda c: c.data == "final_leaderboard", state='*')
async def cb_final_leaderboard(callback_query: types.CallbackQuery):
    cid = callback_query.message.chat.id
    await callback_query.answer()
    try:
        data = await api_get("/leaderboard")
        items = data.get("items", [])
        header = f"🏆 Лидерборд (private)"
        if not items:
            text = f"{header}\nЛидерборд пока пуст"
        else:
            lines = []
            lines.append(f"{'#':>2}  {'Команда':<20}  {'F1':>6}  {'Latency, ms':>12}")
            lines.append("-" * 46)
            for idx, it in enumerate(items, start=1):
                name = str(it.get('team_name', ''))[:20]
                f1_val = it.get('f1', None)
                lat_val = it.get('avg_latency_ms', None)
                f1_str = '-' if f1_val is None else f"{float(f1_val):.4f}"
                lat_str = '-' if lat_val is None else f"{float(lat_val):.1f}"
                lines.append(f"{idx:>2}.  {name:<20}  {f1_str:>6}  {lat_str:>12}")
            text = header + "\n" + "```\n" + "\n".join(lines) + "\n```"
        await bot.send_message(cid, text, reply_markup=kb_registered(), parse_mode="Markdown")
    except BackendError as e:
        await bot.send_message(cid, f"Ошибка получения лидерборда: {e.message}", reply_markup=kb_registered())
    except Exception:
        await bot.send_message(cid, "Неожиданная ошибка при получении лидерборда", reply_markup=kb_registered())


@dispatcher.message_handler(commands=["cancel"], state='*')
async def cmd_cancel(message: types.Message, state: FSMContext):
    try:
        await state.finish()
    except Exception:
        pass
    await message.reply("Действие отменено. Выберите действие в меню.", reply_markup=await main_menu_keyboard(message.chat.id))


@dispatcher.callback_query_handler(lambda c: c.data == "cancel_flow", state='*')
async def cb_cancel_flow(callback_query: types.CallbackQuery, state: FSMContext):
    cid = callback_query.message.chat.id
    try:
        await state.finish()
    except Exception:
        pass
    try:
        await callback_query.answer("Отменено")
    except Exception:
        pass
    await bot.send_message(cid, "Действие отменено. Выберите действие в меню.", reply_markup=await main_menu_keyboard(cid))


if __name__ == "__main__":
    executor.start_polling(dispatcher, skip_updates=True)
