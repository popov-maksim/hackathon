import io
import os
import logging
import asyncio

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

# Active progress watchers per chat. Prevents duplicate updaters.
PROGRESS_WATCHERS: dict[int, asyncio.Task] = {}


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


# states
class RegisterStates(StatesGroup):
    waiting_team = State()
    waiting_endpoint = State()


class ChangeEndpointStates(StatesGroup):
    waiting_endpoint = State()


class ChangeGithubStates(StatesGroup):
    waiting_github = State()


class UploadCSVStates(StatesGroup):
    waiting_file = State()


# --- Keyboards ---
def kb_unregistered() -> types.InlineKeyboardMarkup:
    kb = types.InlineKeyboardMarkup(row_width=1)
    kb.add(types.InlineKeyboardButton(text="📝 Регистрация команды", callback_data="register"))
    return kb


def kb_registered() -> types.InlineKeyboardMarkup:
    kb = types.InlineKeyboardMarkup(row_width=2)
    btn_run = types.InlineKeyboardButton(text="▶️ Оценить решение", callback_data="run")
    btn_download = types.InlineKeyboardButton(text="📥 Скачать датасет", callback_data="download_dataset")
    btn_upload = types.InlineKeyboardButton(text="📤 Отправить ответы", callback_data="upload_csv")
    btn_results = types.InlineKeyboardButton(text="📊 Результаты команды", callback_data="last_result")
    btn_lb = types.InlineKeyboardButton(text="🏆 Лидерборд", callback_data="leaderboard")
    btn_change = types.InlineKeyboardButton(text="🔧 Сменить URL сервиса", callback_data="change_endpoint")
    btn_change_github = types.InlineKeyboardButton(text="🔧 Сменить GitHub ссылку", callback_data="change_github")

    # 1-й ряд: одна кнопка
    kb.row(btn_run)
    # 2-й ряд: две кнопки
    kb.row(btn_download, btn_upload)
    # 3-й ряд: две кнопки
    kb.row(btn_results, btn_lb)
    # 4-й ряд: две кнопки
    kb.row(btn_change, btn_change_github)
    return kb


def kb_cancel_inline() -> types.InlineKeyboardMarkup:
    kb = types.InlineKeyboardMarkup(row_width=1)
    kb.add(types.InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_flow"))
    return kb


def kb_confirm_run() -> types.InlineKeyboardMarkup:
    kb = types.InlineKeyboardMarkup(row_width=2)
    kb.row(
        types.InlineKeyboardButton(text="🚀 Запустить", callback_data="confirm_run"),
        types.InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_flow"),
    )
    return kb


def kb_confirm_download() -> types.InlineKeyboardMarkup:
    kb = types.InlineKeyboardMarkup(row_width=2)
    kb.row(
        types.InlineKeyboardButton(text="⬇️ Скачать", callback_data="confirm_download_dataset"),
        types.InlineKeyboardButton(text="❌ Отмена", callback_data="cancel_flow"),
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
        gh = team.get('github_url')
        url_line = f"\nТекущий URL: {url}" if url else ""
        gh_line = f"\nТекущий GitHub: {gh}" if gh else ""
        text = f"Команда: {team.get('name')}.{url_line}{gh_line}\nВыберите действие:"
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
    await state.update_data(team_name=team)
    await message.reply("Теперь введите IP или URL вашего сервиса (например, 1.2.3.4:8000 или https://host).", reply_markup=kb_cancel_inline())
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
        resp = await api_post(
            "/teams/register",
            {"tg_chat_id": message.chat.id, "team_name": team_name, "endpoint_url": endpoint},
        )
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
    # Подтверждение запуска
    await bot.send_message(cid, "Запустить оценку сейчас?", reply_markup=kb_confirm_run())


@dispatcher.callback_query_handler(lambda c: c.data == "confirm_run", state='*')
async def cb_confirm_run(callback_query: types.CallbackQuery):
    cid = callback_query.message.chat.id
    await callback_query.answer()
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
    status_emoji = {"queued": "⏳", "running": "🔄", "done": "✅"}

    # 1) Проверим регистрацию команды
    try:
        team = await api_get(f"/teams/{cid}")
    except BackendError as e:
        if e.status == 404:
            return await bot.send_message(cid, "Сначала зарегистрируйте команду.", reply_markup=kb_unregistered())
        return await bot.send_message(cid, f"Не удалось получить данные команды: {e.message}")
    except Exception:
        return await bot.send_message(cid, "Неожиданная ошибка при получении данных команды")

    # 2) Последний онлайн-запуск (а также текущий статус)
    last = None
    try:
        last = await api_get(f"/teams/{cid}/last_run")
    except BackendError as e:
        if e.status == 404:
            # Вообще не было запусков — покажем блок Online с прочерками
            last = None
        else:
            return await bot.send_message(cid, f"Ошибка получения результатов: {e.message}", reply_markup=kb_registered())
    except Exception:
        return await bot.send_message(cid, "Неожиданная ошибка при получении результатов", reply_markup=kb_registered())

    # 3) Лидерборд — найдём лучшее онлайн‑решение и позицию
    best_block_lines: list[str] = []
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
            best_f1 = my_item.get('f1')
            best_lat = my_item.get('avg_latency_ms')
            best_block_lines = [
                "🏅 Лучшая отправка:",
                f"├─ F1: `{fmt_f1(best_f1)}`",
                f"└─ Latency: `{fmt_lat(best_lat)}`",
            ]
            rank_line = f"Место в лидерборде: {my_idx} из {len(items)}"
    except BackendError:
        pass
    except Exception:
        pass

    # 4) Онлайн блок
    cur_status = str(last.get("status")) if last else ""
    is_active = (cur_status in ("queued", "running")) if last else False
    header = "📊 *Результаты команды*"

    if is_active:
        st = status_map.get(cur_status, cur_status)
        st_emoji = status_emoji.get(cur_status, "ℹ️")
        status_line = f"{st_emoji} Статус: {st}"
        run_line = f"`run_id={last.get('run_id')}`\nУспешно/Тотал`{last.get('samples_success')}/{last.get('samples_total')}`"
    else:
        status_line = "ℹ️ Статус: Сейчас нет активной оценки"
        run_line = None

    pb_line = None
    if is_active:
        pb = progress_bar(last.get("samples_processed", 0) or 0, last.get("samples_total", 0) or 0)
        if pb:
            pb_line = f"Прогресс: {pb}"

    last_f1 = (last.get("f1") if last and cur_status == "done" else None)
    last_lat = (last.get("avg_latency_ms") if last and cur_status == "done" else None)
    # Добавляем долю успешных в виде succeed/total к результатам
    if last and cur_status == "done":
        succ = last.get("samples_success", 0) or 0
        tot = last.get("samples_total", 0) or 0
        last_block_lines = [
            "🧪 Последняя отправка:",
            f"├─ F1: `{fmt_f1(last_f1)}`",
            f"├─ Успешно/Тотал: `{int(succ)}/{int(tot)}`",
            f"└─ Latency: `{fmt_lat(last_lat)}`",
        ]
    else:
        last_block_lines = [
            "🧪 Последняя отправка:",
            f"├─ F1: `{fmt_f1(last_f1)}`",
            f"└─ Latency: `{fmt_lat(last_lat)}`",
        ]

    sep = "━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    lines = [header, sep, "_📡 Online метрики_", ""]
    lines.append(status_line)
    if run_line:
        lines.append(run_line)
    if pb_line:
        lines.append(pb_line)
    lines.append("")
    lines.extend(last_block_lines)
    if best_block_lines:
        lines.append("")
        lines.extend(best_block_lines)
    if rank_line:
        lines.append(f"🏆 {rank_line}")

    # 5) Оффлайн блок
    lines.append("")
    lines.append(sep)
    lines.append("🧾 _Offline метрики_")
    lines.append("")
    offline_status_line = "ℹ️ Статус: Пока нет оффлайн-оценок"
    offline_last_lines: list[str] = []
    offline_best_lines: list[str] = []

    try:
        last_csv = await api_get(f"/teams/{cid}/last_csv")
        st = str(last_csv.get("status"))
        if st == "done":
            offline_status_line = "✅ Статус: Завершено"
        elif st in ("queued", "running"):
            offline_status_line = "🔄 Статус: Выполняется"
        else:
            offline_status_line = f"ℹ️ Статус: {st}"
        offline_last_lines = [
            "🧪 Последняя отправка:",
            f"└─ F1: `{fmt_f1(last_csv.get('f1'))}`",
        ]
    except BackendError as e:
        if e.status != 404:
            offline_status_line = f"ℹ️ Статус: {e.message}"
    except Exception:
        pass

    try:
        best_csv = await api_get(f"/teams/{cid}/best_csv")
        offline_best_lines = [
            "🏅 Лучшая отправка:",
            f"└─ F1: `{fmt_f1(best_csv.get('f1'))}`",
        ]
    except BackendError:
        # нет лучших (не было завершённых)
        pass
    except Exception:
        pass

    lines.append(offline_status_line)
    if offline_last_lines:
        lines.append("")
        lines.extend(offline_last_lines)
    if offline_best_lines:
        lines.append("")
        lines.extend(offline_best_lines)

    msg = await bot.send_message(cid, "\n".join(lines), reply_markup=kb_registered(), parse_mode="Markdown")
    # Auto-update progress if running
    if is_active and pb_line:
        old = PROGRESS_WATCHERS.get(cid)
        if old and not old.done():
            old.cancel()
        PROGRESS_WATCHERS[cid] = asyncio.create_task(_watch_and_update_results(cid, msg.message_id))


@dispatcher.callback_query_handler(lambda c: c.data == "download_dataset", state='*')
async def cb_download_dataset(callback_query: types.CallbackQuery):
    cid = callback_query.message.chat.id
    await callback_query.answer()
    # Подтверждение скачивания
    await bot.send_message(cid, "Скачать текущий датасет?", reply_markup=kb_confirm_download())


@dispatcher.callback_query_handler(lambda c: c.data == "confirm_download_dataset", state='*')
async def cb_confirm_download_dataset(callback_query: types.CallbackQuery):
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
            caption="Файл готов для скачивания",
        )
        # Отправим клавиатуру отдельным текстовым сообщением, чтобы избежать сужения кнопок
        await bot.send_message(cid, "Выберите действие:", reply_markup=kb_registered())
    except BackendError as e:
        await bot.send_message(cid, f"Ошибка загрузки датасета: {e.message}", reply_markup=kb_registered())
    except Exception:
        await bot.send_message(cid, "Неожиданная ошибка при загрузке датасета", reply_markup=kb_registered())


@dispatcher.callback_query_handler(lambda c: c.data == "upload_csv", state='*')
async def cb_upload_csv(callback_query: types.CallbackQuery, state: FSMContext):
    cid = callback_query.message.chat.id
    await callback_query.answer()
    # Закроем любой предыдущий flow
    try:
        await state.finish()
    except Exception:
        pass
    await bot.send_message(
        cid,
        "Пришлите CSV-файл с вашими предсказаниями (столбец 'annotation', разделитель ';').",
        reply_markup=kb_cancel_inline(),
    )
    await UploadCSVStates.waiting_file.set()


@dispatcher.message_handler(content_types=[types.ContentType.DOCUMENT], state=UploadCSVStates.waiting_file)
async def st_upload_csv_file(message: types.Message, state: FSMContext):
    cid = message.chat.id
    doc = message.document
    if not doc or not str(doc.file_name or "").lower().endswith(".csv"):
        return await message.reply("Нужен CSV-файл. Попробуйте снова или /cancel для отмены.")
    try:
        # Получим путь к файлу в Telegram
        tg_file = await bot.get_file(doc.file_id)
        file_path = tg_file.file_path
        # Скачаем байты файла
        async with httpx.AsyncClient(timeout=60.0) as client:
            url = f"https://api.telegram.org/file/bot{BOT_TOKEN}/{file_path}"
            resp = await client.get(url)
            resp.raise_for_status()
            file_bytes = resp.content

        files = {"file": (doc.file_name or "predictions.csv", file_bytes, "text/csv")}
        data = {"tg_chat_id": str(cid)}
        res = await api_post_multipart("/runs_csv/upload", data=data, files=files)
        await message.reply(
            f"Откройте '📊 Результаты команды' → Offline метрики, чтобы посмотреть статус/результаты.",
            reply_markup=kb_registered(),
        )
        await state.finish()
    except BackendError as e:
        await message.reply(f"Ошибка загрузки: {e.message}", reply_markup=kb_registered())
        await state.finish()
    except Exception:
        await message.reply("Неожиданная ошибка при загрузке файла", reply_markup=kb_registered())
        await state.finish()


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


async def _build_results_text_and_active(cid: int) -> tuple[str, bool]:
    def fmt_f1(v):
        try:
            return f"{float(v):.4f}" if v is not None else "—"
        except Exception:
            return "—"

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
    status_emoji = {"queued": "⏳", "running": "🔄", "done": "✅"}

    # 1) Team
    try:
        team = await api_get(f"/teams/{cid}")
    except BackendError as e:
        if e.status == 404:
            return ("Сначала зарегистрируйте команду.", False)
        return (f"Не удалось получить данные команды: {e.message}", False)
    except Exception:
        return ("Неожиданная ошибка при получении данных команды", False)

    # 2) Last run
    try:
        last = await api_get(f"/teams/{cid}/last_run")
    except BackendError as e:
        if e.status == 404:
            last = None
        else:
            return (f"Ошибка получения результатов: {e.message}", False)
    except Exception:
        return ("Неожиданная ошибка при получении результатов", False)

    # 3) Leaderboard best and rank
    best_block_lines: list[str] = []
    rank_line = ""
    try:
        lb = await api_get("/leaderboard")
        items = lb.get("items", [])
        my_idx = None
        my_item = None
        for idx, it in enumerate(items, start=1):
            if str(it.get("team_name")) == str(team.get("name")):
                my_idx = idx
                my_item = it
                break
        if my_item is not None:
            best_f1 = my_item.get('f1')
            best_lat = my_item.get('avg_latency_ms')
            best_block_lines = [
                "🏅 Лучшая отправка:",
                f"├─ F1: `{fmt_f1(best_f1)}`",
                f"└─ Latency: `{fmt_lat(best_lat)}`",
            ]
            rank_line = f"Моё место в лидерборде: {my_idx} из {len(items)}"
    except BackendError:
        pass
    except Exception:
        pass

    # 4) Online block
    cur_status = str(last.get("status")) if last else ""
    is_active = (cur_status in ("queued", "running")) if last else False
    header = "📊 *Результаты команды*"

    if is_active:
        st = status_map.get(cur_status, cur_status)
        st_emoji = status_emoji.get(cur_status, "ℹ️")
        status_line = f"{st_emoji} Статус: {st}"
        run_line = f"Запуск: `run_id={last.get('run_id')}`  `{last.get('samples_success')}/{last.get('samples_total')}`"
    else:
        status_line = "ℹ️ Статус: Сейчас нет активной оценки"
        run_line = None

    pb_line = None
    if is_active:
        pb = progress_bar(last.get("samples_processed", 0) or 0, last.get("samples_total", 0) or 0)
        if pb:
            pb_line = f"Прогресс: {pb}"

    last_f1 = (last.get("f1") if last and cur_status == "done" else None)
    last_lat = (last.get("avg_latency_ms") if last and cur_status == "done" else None)
    if last and cur_status == "done":
        succ = last.get("samples_success", 0) or 0
        tot = last.get("samples_total", 0) or 0
        last_block_lines = [
            "🧪 Последняя отправка:",
            f"├─ F1: `{fmt_f1(last_f1)}`",
            f"├─ Успешно: `{int(succ)}/{int(tot)}`",
            f"└─ Latency: `{fmt_lat(last_lat)}`",
        ]
    else:
        last_block_lines = [
            "🧪 Последняя отправка:",
            f"├─ F1: `{fmt_f1(last_f1)}`",
            f"└─ Latency: `{fmt_lat(last_lat)}`",
        ]

    sep = "━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    lines = [header, sep, "_📡 Online метрики_", ""]
    lines.append(status_line)
    if run_line:
        lines.append(run_line)
    if pb_line:
        lines.append(pb_line)
    lines.append("")
    lines.extend(last_block_lines)
    if best_block_lines:
        lines.append("")
        lines.extend(best_block_lines)
    if rank_line:
        lines.append(f"🏆 {rank_line}")

    # 5) Offline block
    lines.append("")
    lines.append(sep)
    lines.append("🧾 _Offline метрики_")
    lines.append("")
    offline_status_line = "ℹ️ Статус: Пока нет оффлайн-оценок"
    offline_last_lines: list[str] = []
    offline_best_lines: list[str] = []

    try:
        last_csv = await api_get(f"/teams/{cid}/last_csv")
        st = str(last_csv.get("status"))
        if st == "done":
            offline_status_line = "✅ Статус: Завершено"
        elif st in ("queued", "running"):
            offline_status_line = "🔄 Статус: Выполняется"
        else:
            offline_status_line = f"ℹ️ Статус: {st}"
        offline_last_lines = [
            "🧪 Последняя отправка:",
            f"└─ F1: `{fmt_f1(last_csv.get('f1'))}`",
        ]
    except BackendError as e:
        if e.status != 404:
            offline_status_line = f"ℹ️ Статус: {e.message}"
    except Exception:
        pass

    try:
        best_csv = await api_get(f"/teams/{cid}/best_csv")
        offline_best_lines = [
            "🏅 Лучшая отправка:",
            f"└─ F1: `{fmt_f1(best_csv.get('f1'))}`",
        ]
    except BackendError:
        pass
    except Exception:
        pass

    lines.append(offline_status_line)
    if offline_last_lines:
        lines.append("")
        lines.extend(offline_last_lines)
    if offline_best_lines:
        lines.append("")
        lines.extend(offline_best_lines)

    text = "\n".join(lines)
    should_watch = bool(is_active and pb_line)
    return text, should_watch


async def _watch_and_update_results(cid: int, message_id: int):
    prev_text = None
    try:
        for _ in range(180):  # ~6 минут при 2с интервале
            text, cont = await _build_results_text_and_active(cid)
            if prev_text != text:
                try:
                    await bot.edit_message_text(text, chat_id=cid, message_id=message_id, reply_markup=kb_registered(), parse_mode="Markdown")
                except Exception:
                    pass
                prev_text = text
            if not cont:
                break
            await asyncio.sleep(2.0)
    finally:
        task = PROGRESS_WATCHERS.get(cid)
        if task and task is asyncio.current_task():
            PROGRESS_WATCHERS.pop(cid, None)
@dispatcher.callback_query_handler(lambda c: c.data == "last_csv_result", state='*')
async def cb_last_csv_result(callback_query: types.CallbackQuery):
    cid = callback_query.message.chat.id
    await callback_query.answer()
    try:
        data = await api_get(f"/teams/{cid}/last_csv")
        status = str(data.get("status"))
        f1 = data.get("f1")
        if status == "done":
            msg = f"🧾 Оффлайн оценка: F1 = {float(f1):.4f}"
        else:
            msg = "🧾 Оффлайн оценка: выполняется…"
        await bot.send_message(cid, msg, reply_markup=kb_registered())
    except BackendError as e:
        if e.status == 404:
            await bot.send_message(cid, "Пока нет оффлайн-оценок. Загрузите CSV предсказаний.", reply_markup=kb_registered())
        else:
            await bot.send_message(cid, f"Ошибка получения оффлайн-результата: {e.message}", reply_markup=kb_registered())
    except Exception:
        await bot.send_message(cid, "Неожиданная ошибка при получении оффлайн-результата", reply_markup=kb_registered())


@dispatcher.callback_query_handler(lambda c: c.data == "change_endpoint", state='*')
async def cb_change_endpoint(callback_query: types.CallbackQuery, state: FSMContext):
    cid = callback_query.message.chat.id
    await callback_query.answer()
    # Закрываем любой предыдущий flow перед началом смены URL
    try:
        await state.finish()
    except Exception:
        pass
    await bot.send_message(cid, "Введите новый IP или URL вашего сервиса (например, 1.2.3.4:8000 или https://host).", reply_markup=kb_cancel_inline())
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


@dispatcher.callback_query_handler(lambda c: c.data == "change_github", state='*')
async def cb_change_github(callback_query: types.CallbackQuery, state: FSMContext):
    cid = callback_query.message.chat.id
    await callback_query.answer()
    # Закрываем любой предыдущий flow перед началом смены GitHub ссылки
    try:
        await state.finish()
    except Exception:
        pass
    await bot.send_message(
        cid,
        "Введите ссылку на GitHub репозиторий (например, https://github.com/user/repo).",
        reply_markup=kb_cancel_inline(),
    )
    await ChangeGithubStates.waiting_github.set()


@dispatcher.message_handler(state=ChangeGithubStates.waiting_github)
async def st_change_github(message: types.Message, state: FSMContext):
    cid = message.chat.id
    if not message.text or not isinstance(message.text, str):
        return await message.reply("Пожалуйста, отправьте ссылку текстом. Или /cancel для отмены.")
    if message.text.startswith('/'):
        return await message.reply("Это похоже на команду. Отправьте ссылку текстом или используйте /cancel.")
    gh = message.text.strip()
    if not (gh.startswith("http://") or gh.startswith("https://")):
        gh = "https://" + gh
    try:
        team = await api_get(f"/teams/{cid}")
        # Передаём текущий endpoint_url, чтобы не изменить его
        payload = {
            "tg_chat_id": cid,
            "team_name": team["name"],
            "endpoint_url": team.get("endpoint_url", ""),
            "github_url": gh,
        }
        resp = await api_post("/teams/register", payload)
        cur_gh = resp.get('github_url', gh)
        await message.reply(
            f"Готово. Обновлена GitHub ссылка для команды: {resp['name']}\nТекущий GitHub: {cur_gh}",
            reply_markup=kb_registered(),
        )
        await state.finish()
    except BackendError as e:
        if e.status in (400, 422):
            await message.reply(f"Не удалось обновить GitHub ссылку: {e.message}\nВведите корректную ссылку или /cancel для отмены.")
            return
        await message.reply(f"Не удалось обновить GitHub ссылку: {e.message}", reply_markup=kb_registered())
        await state.finish()
    except Exception:
        await message.reply("Неожиданная ошибка при обновлении GitHub ссылки", reply_markup=kb_registered())
        await state.finish()


# --- Cancel handler ---
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
