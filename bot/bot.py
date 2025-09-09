import io
import os
import logging

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
    # FastAPI HTTPException: {"detail": "..."} или {"detail": [{...}]}
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
    # Валидационные ошибки могут приходить как список
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


# states
class RegisterStates(StatesGroup):
    waiting_team = State()
    waiting_endpoint = State()


class ChangeEndpointStates(StatesGroup):
    waiting_endpoint = State()


# --- Keyboards ---
def kb_unregistered() -> types.InlineKeyboardMarkup:
    kb = types.InlineKeyboardMarkup(row_width=1)
    kb.add(types.InlineKeyboardButton(text="📝 Регистрация команды", callback_data="register"))
    return kb


def kb_registered() -> types.InlineKeyboardMarkup:
    kb = types.InlineKeyboardMarkup(row_width=1)
    kb.add(
        types.InlineKeyboardButton(text="▶️ Оценить решение", callback_data="run"),
        types.InlineKeyboardButton(text="📊 Мои результаты", callback_data="last_result"),
        types.InlineKeyboardButton(text="📥 Скачать датасет", callback_data="download_dataset"),
        types.InlineKeyboardButton(text="🏆 Лидерборд", callback_data="leaderboard"),
        types.InlineKeyboardButton(text="🔧 Сменить URL сервиса", callback_data="change_endpoint"),
    )
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


def _normalize_endpoint(s: str) -> str:
    s = s.strip()
    if not (s.startswith("http://") or s.startswith("https://")):
        s = "http://" + s
    if not s.endswith("/api/predict"):
        s = s.rstrip("/") + "/api/predict"
    return s


# --- /start ---
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
        url = team.get('endpoint_url')
        url_line = f"\nТекущий URL: {url}" if url else ""
        text = f"Готово! Команда: {team.get('name')}.{url_line}\nВыберите действие:"
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
    await message.reply(text, reply_markup=kb)


# --- Callbacks: registration flow (2 steps) ---
@dispatcher.callback_query_handler(lambda c: c.data == "register", state='*')
async def cb_register(callback_query: types.CallbackQuery, state: FSMContext):
    await callback_query.answer()
    # Закрываем любой предыдущий flow перед началом регистрации
    try:
        await state.finish()
    except Exception:
        pass
    await bot.send_message(callback_query.message.chat.id, "Введите название команды:")
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
    await state.update_data(team_name=team)
    await message.reply("Теперь введите IP или URL вашего сервиса (например, 1.2.3.4:8000 или https://host):")
    await RegisterStates.waiting_endpoint.set()


@dispatcher.message_handler(state=RegisterStates.waiting_endpoint)
async def st_register_endpoint(message: types.Message, state: FSMContext):
    if not message.text or not isinstance(message.text, str):
        return await message.reply("Пожалуйста, отправьте URL текстом. Или /cancel для отмены.")
    if message.text.startswith('/'):
        return await message.reply("Это похоже на команду. Отправьте URL текстом или используйте /cancel.")
    endpoint = _normalize_endpoint(message.text)
    data = await state.get_data()
    team_name = data.get("team_name")
    try:
        resp = await api_post("/teams/register", {"tg_chat_id": message.chat.id, "team_name": team_name, "endpoint_url": endpoint})
        await message.reply(
            f"Регистрация завершена.\nНазвание команды: {resp['name']}\nТекущий URL: {resp.get('endpoint_url', endpoint)}",
            reply_markup=kb_registered()
        )
        await state.finish()
    except BackendError as e:
        # Для ошибок валидации оставляем пользователя в том же шаге
        if e.status in (400, 422):
            await message.reply(f"Ошибка регистрации: {e.message}\nВведите корректный URL или /cancel для отмены.")
            return
        await message.reply(f"Ошибка регистрации: {e.message}", reply_markup=kb_unregistered())
        await state.finish()
    except Exception:
        await message.reply("Неожиданная ошибка при регистрации", reply_markup=kb_unregistered())
        await state.finish()


# --- Callbacks: run check and last result ---
@dispatcher.callback_query_handler(lambda c: c.data == "run", state='*')
async def cb_run(callback_query: types.CallbackQuery):
    cid = callback_query.message.chat.id
    await callback_query.answer()
    try:
        _ = await api_get(f"/teams/{cid}")
        is_registered = True
    except BackendError as e:
        is_registered = False if e.status == 404 else True
    except Exception:
        is_registered = False
    if not is_registered:
        return await bot.send_message(cid, "Сначала зарегистрируйте команду.", reply_markup=kb_unregistered())
    try:
        data = await api_post("/runs/start", {"tg_chat_id": cid})
        await bot.send_message(cid, f"Запущен тест: run_id={data['run_id']}, status={data['status']}", reply_markup=kb_registered())
    except BackendError as e:
        await bot.send_message(cid, f"Ошибка запуска: {e.message}", reply_markup=kb_registered())
    except Exception:
        await bot.send_message(cid, "Неожиданная ошибка при запуске", reply_markup=kb_registered())


@dispatcher.callback_query_handler(lambda c: c.data == "last_result", state='*')
async def cb_last_result(callback_query: types.CallbackQuery):
    cid = callback_query.message.chat.id
    await callback_query.answer()

    def fmt_f1(v):
        return f"{float(v):.4f}" if v is not None else "—"

    def fmt_lat(v):
        try:
            return f"{float(v):.1f} ms" if v is not None else "—"
        except Exception:
            return "—"

    def progress_bar(done: int, total: int, width: int = 20) -> str | None:
        try:
            td = int(done)
            tt = int(total)
        except Exception:
            return None
        if tt <= 0:
            return None
        ratio = max(0.0, min(1.0, (td / tt)))
        filled = int(ratio * width)
        empty = width - filled
        bar = "█" * filled + "░" * empty
        percent = int(ratio * 100)
        return f"[{bar}] {percent}%"

    status_map = {"queued": "В очереди", "running": "Выполняется", "done": "Завершено"}

    # 1) Проверим регистрацию команды
    try:
        team = await api_get(f"/teams/{cid}")
    except BackendError as e:
        if e.status == 404:
            return await bot.send_message(cid, "Сначала зарегистрируйте команду.", reply_markup=kb_unregistered())
        return await bot.send_message(cid, f"Не удалось получить данные команды: {e.message}")
    except Exception:
        return await bot.send_message(cid, "Неожиданная ошибка при получении данных команды")

    # 2) Последний запуск (а также текущий статус)
    last = None
    try:
        last = await api_get(f"/teams/{cid}/last_run")
    except BackendError as e:
        if e.status == 404:
            # Вообще не было запусков
            return await bot.send_message(
                cid,
                "Пока нет ни одной оценки. Запустите проверку — всё получится! 🙂",
                reply_markup=kb_registered(),
            )
        return await bot.send_message(cid, f"Ошибка получения результатов: {e.message}", reply_markup=kb_registered())
    except Exception:
        return await bot.send_message(cid, "Неожиданная ошибка при получении результатов", reply_markup=kb_registered())

    # 3) Лидерборд — найдём лучшее решение и позицию
    best_line = ""
    rank_line = ""
    try:
        lb = await api_get("/leaderboard")
        items = lb.get("items", [])
        # Найти строку для команды
        my_idx = None
        my_item = None
        for idx, it in enumerate(items, start=1):
            if str(it.get("team_name")) == str(team.get("name")):
                my_idx = idx
                my_item = it
                break
        if my_item is not None:
            best_line = f"Лучшее решение (F1): F1={fmt_f1(my_item.get('f1'))}, Latency={fmt_lat(my_item.get('avg_latency_ms'))}"
            rank_line = f"Моё место в лидерборде: {my_idx} из {len(items)}"
    except BackendError:
        pass
    except Exception:
        pass

    # 4) Соберём текст
    cur_status = str(last.get("status"))
    is_active = cur_status in ("queued", "running")
    status_line = (
        f"Текущая оценка: {status_map.get(cur_status, cur_status)} (run_id={last.get('run_id')}, {last.get('samples_success')}/{last.get('samples_total')})"
        if is_active else "Сейчас нет активной оценки"
    )
    pb_line = None
    if is_active:
        pb = progress_bar(last.get("samples_success", 0) or 0, last.get("samples_total", 0) or 0)
        if pb:
            pb_line = f"Доля успешно отработанных: {pb}"

    last_f1 = last.get("f1") if cur_status == "done" else None
    last_lat = last.get("avg_latency_ms") if cur_status == "done" else None
    last_line = f"Последняя отправка: F1={fmt_f1(last_f1)}, Latency={fmt_lat(last_lat)}"

    lines = [
        "Мои результаты",
        status_line,
        last_line,
    ]
    if pb_line:
        lines.insert(2, pb_line)
    if best_line:
        lines.append(best_line)
    if rank_line:
        lines.append(rank_line)

    await bot.send_message(cid, "\n".join(lines), reply_markup=kb_registered())


@dispatcher.callback_query_handler(lambda c: c.data == "download_dataset", state='*')
async def cb_download_dataset(callback_query: types.CallbackQuery):
    cid = callback_query.message.chat.id
    await callback_query.answer()
    try:
        async with httpx.AsyncClient(timeout=20.0) as client:
            try:
                r = await client.get(API_BASE_URL + "/phases/current/dataset", params={"tg_chat_id": cid})
                r.raise_for_status()
            except httpx.HTTPStatusError as e:
                raise BackendError(_extract_backend_error(e.response), e.response.status_code)
            except httpx.RequestError:
                raise BackendError("Сервис API недоступен. Проверьте URL и доступность.")
            data = r.content
        await bot.send_document(
            cid,
            types.InputFile(io.BytesIO(data), filename="dataset.csv"),
            caption="Готов для скачивания",
            reply_markup=kb_registered(),
        )
    except BackendError as e:
        await bot.send_message(cid, f"Ошибка загрузки датасета: {e.message}", reply_markup=kb_registered())
    except Exception:
        await bot.send_message(cid, "Неожиданная ошибка при загрузке датасета", reply_markup=kb_registered())


@dispatcher.callback_query_handler(lambda c: c.data == "leaderboard", state='*')
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
    except BackendError as e:
        await bot.send_message(cid, f"Ошибка получения лидерборда: {e.message}", reply_markup=kb_registered())
    except Exception:
        await bot.send_message(cid, "Неожиданная ошибка при получении лидерборда", reply_markup=kb_registered())


@dispatcher.callback_query_handler(lambda c: c.data == "change_endpoint", state='*')
async def cb_change_endpoint(callback_query: types.CallbackQuery, state: FSMContext):
    cid = callback_query.message.chat.id
    await callback_query.answer()
    # Закрываем любой предыдущий flow перед началом смены URL
    try:
        await state.finish()
    except Exception:
        pass
    await bot.send_message(cid, "Введите новый IP или URL вашего сервиса (например, 1.2.3.4:8000 или https://host):")
    await ChangeEndpointStates.waiting_endpoint.set()


@dispatcher.message_handler(state=ChangeEndpointStates.waiting_endpoint)
async def st_change_endpoint(message: types.Message, state: FSMContext):
    cid = message.chat.id
    if not message.text or not isinstance(message.text, str):
        return await message.reply("Пожалуйста, отправьте URL текстом. Или /cancel для отмены.")
    if message.text.startswith('/'):
        return await message.reply("Это похоже на команду. Отправьте URL текстом или используйте /cancel.")
    endpoint = _normalize_endpoint(message.text)
    try:
        team = await api_get(f"/teams/{cid}")
        resp = await api_post(
            "/teams/register",
            {"tg_chat_id": cid, "team_name": team["name"], "endpoint_url": endpoint},
        )
        await message.reply(
            f"Готово. Обновлён URL для команды: {resp['name']}\nТекущий URL: {resp.get('endpoint_url', endpoint)}",
            reply_markup=kb_registered(),
        )
        await state.finish()
    except BackendError as e:
        if e.status in (400, 422):
            await message.reply(f"Не удалось обновить URL: {e.message}\nВведите корректный URL или /cancel для отмены.")
            return
        await message.reply(f"Не удалось обновить URL: {e.message}", reply_markup=kb_registered())
        await state.finish()
    except Exception:
        await message.reply("Неожиданная ошибка при обновлении URL", reply_markup=kb_registered())
        await state.finish()


# --- Cancel handler ---
@dispatcher.message_handler(commands=["cancel"], state='*')
async def cmd_cancel(message: types.Message, state: FSMContext):
    try:
        await state.finish()
    except Exception:
        pass
    await message.reply("Действие отменено. Выберите действие в меню.", reply_markup=await main_menu_keyboard(message.chat.id))


if __name__ == "__main__":
    executor.start_polling(dispatcher, skip_updates=True)
