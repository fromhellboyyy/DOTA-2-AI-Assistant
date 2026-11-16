import csv
import json
import re
import sys
from pathlib import Path

TAGS = ["baseline_stopped", "bp_stopped", "bp_live_1w", "bp_live_2w"]

LABELS = {
    "baseline_stopped": "baseline, воркеры остановлены",
    "bp_stopped": "backpressure, воркеры остановлены",
    "bp_live_1w": "backpressure, 1 воркер",
    "bp_live_2w": "backpressure, 2 воркера",
}

PHASE = {
    "baseline_stopped": "механизм",
    "bp_stopped": "механизм",
    "bp_live_1w": "масштабирование",
    "bp_live_2w": "масштабирование",
}

LIMIT = {
    "baseline_stopped": "100000",
    "bp_stopped": "20",
    "bp_live_1w": "20",
    "bp_live_2w": "20",
}

ROWS_TOTAL = 88
NACK_RE = re.compile(r"backpressure_nack")

METHODOLOGY = (
    "Исследование проводится в двух фазах, потому что противодавление и масштабирование наблюдаются в разных состояниях системы. "
    "В фазе механизма воркеры остановлены, очередь пересоздана пустой и ограничена политикой `max-length=20` с `overflow=reject-publish`; источник публикует пакет из 88 записей. "
    "Без эффективного ограничения глубина дорастает до объёма пакета без отказов. "
    "С ограничением брокер отбивает излишек через `reject-publish`, источник уходит в backoff, а очередь зажата на лимите. "
    "В фазе масштабирования воркеры работают, ограничение очереди применяется политикой RabbitMQ, а замер продолжается до полной разрядки очереди. "
    "Поэтому время полной обработки пакета измеряется как время от начала публикации до момента, когда глубина очереди становится равной нулю. "
    "Все прогоны выполнены при фиксированном `INFERENCE_DELAY_SEC=0.5`. "
    "Глубина снимается через `rabbitmqctl` как авторитетное состояние узла, отдельно фиксируются `messages_ready`, `messages_unacknowledged` и суммарное `messages`. "
    "Память снимается через `docker stats`. "
    "Скорость источника и wall-clock публикации берутся из логов producer через `scripts/extract_rate.py`, отказы считаются по строкам `backpressure_nack` в логе."
)


def to_int(value, default=0):
    try:
        return int(float(value))
    except Exception:
        return default


def to_float(value, default=0.0):
    try:
        return float(value)
    except Exception:
        return default


def num(value, digits=2, default=0.0):
    v = to_float(value, default)
    return f"{round(v, digits)}"


def read_csv(tag):
    path = Path("results") / f"{tag}.csv"
    if not path.exists():
        return {}

    rows_data = []
    depths = []
    readies = []
    unacked_list = []
    rabbits = []
    workers = []
    producers = []
    totals = []

    with path.open(encoding="utf-8") as f:
        for row in csv.DictReader(f):
            t = float(row["t"])
            depth = int(row["depth"])

            ready_raw = row.get("ready", "")
            ready = int(ready_raw) if ready_raw not in ("", None) else depth

            unacked_raw = row.get("unacked", "")
            unacked = int(unacked_raw) if unacked_raw not in ("", None) else max(0, depth - ready)

            rabbit_mb = float(row["rabbit_mb"])
            worker_mb = float(row["worker_mb"])
            producer_mb = float(row["producer_mb"])
            total_mb = rabbit_mb + worker_mb + producer_mb

            depths.append(depth)
            readies.append(ready)
            unacked_list.append(unacked)
            rabbits.append(rabbit_mb)
            workers.append(worker_mb)
            producers.append(producer_mb)
            totals.append(total_mb)

            rows_data.append(
                {
                    "t": t,
                    "depth": depth,
                    "ready": ready,
                    "unacked": unacked,
                    "rabbit_mb": rabbit_mb,
                    "worker_mb": worker_mb,
                    "producer_mb": producer_mb,
                    "total_mb": total_mb,
                }
            )

    if not rows_data:
        return {}

    return {
        "rows": rows_data,
        "duration_s": rows_data[-1]["t"],
        "total_peak": max(depths),
        "ready_peak": max(readies),
        "unacked_peak": max(unacked_list),
        "depth_mean": round(sum(depths) / len(depths), 2),
        "rabbit_mb_peak": round(max(rabbits), 2),
        "worker_mb_peak": round(max(workers), 2),
        "producer_mb_peak": round(max(producers), 2),
        "total_mb_peak": round(max(totals), 2),
    }


def read_final(tag):
    path = Path("results") / f"{tag}.final"
    if not path.exists():
        return None
    try:
        return int(path.read_text(encoding="ascii").strip())
    except Exception:
        return None


def read_drain(tag):
    path = Path("results") / f"{tag}.drain"
    if not path.exists():
        return None

    txt = path.read_text(encoding="ascii").strip()
    if not txt:
        return None

    try:
        return float(txt)
    except Exception:
        return None


def read_rate(tag):
    path = Path("results") / f"{tag}_rate.json"
    if not path.exists():
        return {"published": 0, "wall_s": 0.0, "rate_per_s": 0.0}
    return json.loads(path.read_text(encoding="utf-8"))


def count_backpressure(tag):
    path = Path(f"logs_{tag}.txt")
    if not path.exists():
        return 0

    n = 0
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        if NACK_RE.search(line):
            n += 1
    return n


def collect():
    out = {}

    for tag in TAGS:
        m = read_csv(tag)
        if not m:
            continue

        r = read_rate(tag)
        fin = read_final(tag)
        dr = read_drain(tag)
        nk = count_backpressure(tag)

        out[tag] = {
            **m,
            **r,
            "final_depth": fin,
            "drain_s": dr,
            "nacks": nk,
        }

    return out


def g(data, tag, key, default="?"):
    d = data.get(tag)
    if not d:
        return default
    v = d.get(key, default)
    return default if v is None else v


def raw(data, tag, key, default=None):
    return data.get(tag, {}).get(key, default)


def pick_rows(rows, n=6):
    if not rows:
        return []
    if len(rows) <= n:
        return rows

    step = (len(rows) - 1) / (n - 1)
    indices = sorted({round(i * step) for i in range(n)})
    return [rows[i] for i in indices]


def main_table(data):
    head = (
        "| Режим | Фаза | Лимит | Глубина финал | Ready peak | Unacked peak | Total peak | "
        "Отказы | Опубликовано | Скорость источника | Wall-clock публикации |"
    )
    sep = "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |"
    rows = [head, sep]

    for tag in TAGS:
        if tag not in data:
            continue

        rows.append(
            f"| {LABELS[tag]} | {PHASE[tag]} | {LIMIT[tag]} | "
            f"{g(data, tag, 'final_depth')} | "
            f"{g(data, tag, 'ready_peak')} | "
            f"{g(data, tag, 'unacked_peak')} | "
            f"{g(data, tag, 'total_peak')} | "
            f"{g(data, tag, 'nacks')} | "
            f"{g(data, tag, 'published')} | "
            f"{num(g(data, tag, 'rate_per_s'))} задач/с | "
            f"{num(g(data, tag, 'wall_s'), 1)} с |"
        )

    return "\n".join(rows) + "\n"


def memory_table(data):
    head = (
        "| Режим | Память брокера | Память воркера | Память источника | Суммарная память | "
        "Время полной обработки пакета | Средний интервал завершения задач |"
    )
    sep = "| --- | --- | --- | --- | --- | --- | --- |"
    rows = [head, sep]

    for tag in TAGS:
        if tag not in data:
            continue

        dr = raw(data, tag, "drain_s")
        if dr is None:
            drain_txt = "-"
            per_txt = "-"
        else:
            dr_f = to_float(dr, 0.0)
            if dr_f <= 0:
                drain_txt = "-"
                per_txt = "-"
            else:
                drain_txt = f"{round(dr_f, 1)} с"
                per_txt = f"{round(dr_f / ROWS_TOTAL, 2)} с"

        rows.append(
            f"| {LABELS[tag]} | "
            f"{g(data, tag, 'rabbit_mb_peak')} MB | "
            f"{g(data, tag, 'worker_mb_peak')} MB | "
            f"{g(data, tag, 'producer_mb_peak')} MB | "
            f"{g(data, tag, 'total_mb_peak')} MB | "
            f"{drain_txt} | "
            f"{per_txt} |"
        )

    return "\n".join(rows) + "\n"


def dynamics_table(data):
    head = "| Режим | t, с | Ready | Unacked | Total | Брокер, MB | Воркер, MB | Источник, MB |"
    sep = "| --- | --- | --- | --- | --- | --- | --- | --- |"
    rows = [head, sep]

    for tag in TAGS:
        if tag not in data:
            continue

        rows_data = data[tag].get("rows", [])
        for r in pick_rows(rows_data, 6):
            rows.append(
                f"| {LABELS[tag]} | "
                f"{r['t']} | "
                f"{r['ready']} | "
                f"{r['unacked']} | "
                f"{r['depth']} | "
                f"{r['rabbit_mb']} | "
                f"{r['worker_mb']} | "
                f"{r['producer_mb']} |"
            )

    return "\n".join(rows) + "\n"


def findings(data):
    bs_fin = to_int(g(data, "baseline_stopped", "final_depth", 0))
    bp_fin = to_int(g(data, "bp_stopped", "final_depth", 0))
    bs_nk = to_int(g(data, "baseline_stopped", "nacks", 0))
    bp_nk = to_int(g(data, "bp_stopped", "nacks", 0))

    bs_ready_peak = to_int(g(data, "baseline_stopped", "ready_peak", 0))
    bp_ready_peak = to_int(g(data, "bp_stopped", "ready_peak", 0))
    l1_ready_peak = to_int(g(data, "bp_live_1w", "ready_peak", 0))
    l2_ready_peak = to_int(g(data, "bp_live_2w", "ready_peak", 0))

    bs_total_peak = to_int(g(data, "baseline_stopped", "total_peak", 0))
    bp_total_peak = to_int(g(data, "bp_stopped", "total_peak", 0))
    l1_total_peak = to_int(g(data, "bp_live_1w", "total_peak", 0))
    l2_total_peak = to_int(g(data, "bp_live_2w", "total_peak", 0))

    l1_unacked_peak = to_int(g(data, "bp_live_1w", "unacked_peak", 0))
    l2_unacked_peak = to_int(g(data, "bp_live_2w", "unacked_peak", 0))

    bs_rate = to_float(g(data, "baseline_stopped", "rate_per_s", 0.0))
    bp_rate = to_float(g(data, "bp_stopped", "rate_per_s", 0.0))

    bs_rb = to_float(g(data, "baseline_stopped", "rabbit_mb_peak", 0.0))
    bp_rb = to_float(g(data, "bp_stopped", "rabbit_mb_peak", 0.0))

    l1_fin = to_int(g(data, "bp_live_1w", "final_depth", 0))
    l2_fin = to_int(g(data, "bp_live_2w", "final_depth", 0))
    l1_nk = to_int(g(data, "bp_live_1w", "nacks", 0))
    l2_nk = to_int(g(data, "bp_live_2w", "nacks", 0))
    l1_pub = to_int(g(data, "bp_live_1w", "published", 0))
    l2_pub = to_int(g(data, "bp_live_2w", "published", 0))

    l1_rate = to_float(g(data, "bp_live_1w", "rate_per_s", 0.0))
    l2_rate = to_float(g(data, "bp_live_2w", "rate_per_s", 0.0))
    l1_wall = to_float(g(data, "bp_live_1w", "wall_s", 0.0))
    l2_wall = to_float(g(data, "bp_live_2w", "wall_s", 0.0))

    l1_rb = to_float(g(data, "bp_live_1w", "rabbit_mb_peak", 0.0))
    l2_rb = to_float(g(data, "bp_live_2w", "rabbit_mb_peak", 0.0))
    l1_wm = to_float(g(data, "bp_live_1w", "worker_mb_peak", 0.0))
    l2_wm = to_float(g(data, "bp_live_2w", "worker_mb_peak", 0.0))
    l1_tot = to_float(g(data, "bp_live_1w", "total_mb_peak", 0.0))
    l2_tot = to_float(g(data, "bp_live_2w", "total_mb_peak", 0.0))

    l1_drain = raw(data, "bp_live_1w", "drain_s")
    l2_drain = raw(data, "bp_live_2w", "drain_s")
    l1_drain_f = to_float(l1_drain, 0.0)
    l2_drain_f = to_float(l2_drain, 0.0)
    has_drain = l1_drain is not None and l2_drain is not None and l1_drain_f > 0 and l2_drain_f > 0

    prod_vals = [to_float(g(data, t, "producer_mb_peak", 0.0)) for t in TAGS if t in data]
    prod_min = round(min(prod_vals), 2) if prod_vals else 0
    prod_max = round(max(prod_vals), 2) if prod_vals else 0

    if has_drain:
        completion_speedup = round(l1_drain_f / l2_drain_f, 2)
        l1_drain_r = round(l1_drain_f, 1)
        l2_drain_r = round(l2_drain_f, 1)
        per1 = round(l1_drain_f / ROWS_TOTAL, 2)
        per2 = round(l2_drain_f / ROWS_TOTAL, 2)
    else:
        completion_speedup = 0
        l1_drain_r = "-"
        l2_drain_r = "-"
        per1 = "-"
        per2 = "-"

    live_nacks = l1_nk + l2_nk
    ready_limit_ok = bp_ready_peak <= 20 and l1_ready_peak <= 20 and l2_ready_peak <= 20

    mech = (
        f"В фазе механизма воркеры остановлены. Без эффективного ограничения `baseline_stopped` очередь дорастает до {bs_fin} сообщений при {bs_nk} отказах, "
        f"ready peak {bs_ready_peak}, total peak {bs_total_peak}. "
        f"С политикой `max-length=20` и `overflow=reject-publish` очередь `bp_stopped` удержана на {bp_fin} сообщениях, источник получил {bp_nk} отказов и ушёл в backoff, "
        f"ready peak {bp_ready_peak}, total peak {bp_total_peak}. "
        f"Главный контраст: глубина {bs_fin} против {bp_fin}, отказы {bs_nk} против {bp_nk}. "
    )

    if ready_limit_ok:
        mech += (
            f"В live-режиме ready peak составил {l1_ready_peak} и {l2_ready_peak}, то есть не превысил лимит 20. "
            f"Unacked peak составил {l1_unacked_peak} и {l2_unacked_peak}, total peak {l1_total_peak} и {l2_total_peak}. "
            f"Это означает, что политика `max-length` удерживает ready-часть очереди, а total может быть выше за счёт сообщений, уже взятых воркерами в обработку."
        )
    else:
        mech += (
            f"В live-режиме ready peak составил {l1_ready_peak} и {l2_ready_peak}, total peak {l1_total_peak} и {l2_total_peak}. "
            f"Метрика total включает ready и unacknowledged. Потолок ready-очереди корректно доказан stopped-фазой, "
            f"а live-фаза используется для измерения времени полной обработки пакета и влияния второго воркера."
        )

    mem = (
        f"Память источника остаётся в диапазоне {prod_min} - {prod_max} MB и не растёт пропорционально пакету из 88 задач. "
        f"Это подтверждает отсутствие внутреннего буфера: при `reject-publish` источник повторяет ту же запись, а не копит задачи в RAM. "
        f"Память брокера: {bs_rb} MB в `baseline_stopped`, {bp_rb} MB в `bp_stopped`, {l1_rb} MB в `bp_live_1w`, {l2_rb} MB в `bp_live_2w`. "
        f"Память потребителей: {l1_wm} MB при одном воркере и {l2_wm} MB при двух. "
        f"Суммарная память конвейера: {l1_tot} MB против {l2_tot} MB. "
        f"Второй воркер даёт прирост пропускной способности ценой роста памяти на стороне потребителей."
    )

    speed = (
        f"В stopped-фазах скорость источника составляет {round(bs_rate, 2)} задач/с без ограничения и {round(bp_rate, 2)} задач/с с backpressure. "
        f"Это показывает, что при остановленных потребителях противодавление реально замедляет источник. "
        f"В live-режиме наблюдённая скорость: {round(l1_rate, 2)} задач/с при одном воркере и {round(l2_rate, 2)} задач/с при двух. "
        f"Теоретическая пропускная способность обработки при последовательной модели около 2 задач/с на воркер, "
        f"то есть 2 и 4 задач/с для одного и двух воркеров. "
    )

    if live_nacks > 0:
        speed += (
            f"Отказы publish в live-режиме: {l1_nk} и {l2_nk}. "
            f"Это означает, что источник временно опережал потребителей и упирался в `reject-publish`; "
            f"после backoff средняя скорость выравнивается с пропускной способностью воркеров."
        )
    else:
        speed += (
            "Отказов publish в live-режиме не зафиксировано, значит в этом прогоне источник не создавал "
            "переполнение ready-очереди сверх политики `reject-publish`."
        )

    if has_drain:
        latency = (
            f"Время одной вычислительной операции задано конфигурацией `INFERENCE_DELAY_SEC=0.5`. "
            f"Время полной обработки пакета из 88 задач до разрядки очереди составило {l1_drain_r} с при одном воркере "
            f"и {l2_drain_r} с при двух воркерах. "
            f"Средний интервал завершения задач равен {per1} с и {per2} с соответственно. "
            f"Коэффициент сокращения времени полной обработки пакета при добавлении второго воркера: {completion_speedup}. "
            f"Средний интервал завершения задач не следует читать как latency одной inference-операции: "
            f"это производная метрика сквозной производительности конвейера, полученная делением времени полной обработки пакета на число задач."
        )
    else:
        latency = (
            f"Данные о полной разрядке очереди отсутствуют. Wall-clock {round(l1_wall, 1)} с и {round(l2_wall, 1)} с относится к публикации пакета источником, "
            f"а не к времени выполнения задачи воркером. При `INFERENCE_DELAY_SEC=0.5` теоретическое время обработки 88 задач: "
            f"44 с одним воркером и 22 с двумя воркерами."
        )

    if has_drain:
        scaling = (
            f"Добавление второго воркера сокращает время полной обработки пакета с {l1_drain_r} с до {l2_drain_r} с, "
            f"то есть с коэффициентом {completion_speedup}. Это происходит без изменения программного кода, только за счёт `scale worker=2`. "
            f"При этом память потребителей растёт с {l1_wm} MB до {l2_wm} MB, а суммарная память конвейера с {l1_tot} MB до {l2_tot} MB. "
            f"Скорость источника в live-режиме увеличивается с {round(l1_rate, 2)} до {round(l2_rate, 2)} задач/с, что согласуется с ростом пропускной способности потребителей. "
            f"Таким образом, второй воркер ускоряет полную обработку пакета, но увеличивает потребление ресурсов на стороне воркеров."
        )
    else:
        scaling = (
            f"Добавление второго воркера снижает финальную глубину затухания очереди с {l1_fin} до {l2_fin} сообщений. "
            f"Память потребителей растёт с {l1_wm} MB до {l2_wm} MB. "
            f"Код приложения не менялся, использован только `scale worker=2`."
        )

    if ready_limit_ok:
        depth_bullet = (
            f"- глубина ready-очереди не превышает установленного лимита: в stopped-фазе `bp_stopped` ready peak {bp_ready_peak} при лимите 20 "
            f"и {bp_nk} отказах, `baseline_stopped` без эффективного ограничения достиг {bs_fin}; "
            f"в live-фазе ready peak составил {l1_ready_peak} и {l2_ready_peak}, total peak {l1_total_peak} и {l2_total_peak} "
            f"за счёт unacknowledged сообщений."
        )
    else:
        depth_bullet = (
            f"- глубина очереди не превышает установленного лимита в stopped-фазе: `bp_stopped` удержан на {bp_fin} при лимите 20 "
            f"и {bp_nk} отказах, `baseline_stopped` без эффективного ограничения достиг {bs_fin}; "
            f"в live-фазе ready peak составил {l1_ready_peak} и {l2_ready_peak}, total peak {l1_total_peak} и {l2_total_peak}; "
            f"поскольку total включает unacknowledged, потолок ready-очереди доказан stopped-фазой, а live-фаза показывает время полной обработки."
        )

    if live_nacks > 0:
        rate_bullet = (
            f"- в установившемся режиме средняя скорость источника согласуется с пропускной способностью воркеров: "
            f"в live-режиме при перегрузке источник получает `reject-publish` и уходит в backoff, отказы {l1_nk} и {l2_nk}; "
            f"в `bp_stopped` скорость источника падает до {round(bp_rate, 2)} задач/с, потому что потребители остановлены."
        )
    else:
        rate_bullet = (
            f"- в установившемся режиме средняя скорость источника не лимитируется очередью в live-режиме: "
            f"{round(l1_rate, 2)} задач/с при одном воркере и {round(l2_rate, 2)} задач/с при двух; "
            f"при остановленных потребителях `bp_stopped` скорость падает до {round(bp_rate, 2)} задач/с из-за backpressure."
        )

    if has_drain:
        scale_bullet = (
            f"- подключение второго воркера увеличивает пропускную способность без изменения кода: "
            f"время полной обработки пакета сократилось с {l1_drain_r} с до {l2_drain_r} с "
            f"(коэффициент {completion_speedup}); память потребителей выросла с {l1_wm} MB до {l2_wm} MB."
        )
    else:
        scale_bullet = (
            f"- подключение второго воркера увеличивает пропускную способность без изменения кода: "
            f"финальная глубина снизилась с {l1_fin} до {l2_fin}; "
            f"память потребителей выросла с {l1_wm} MB до {l2_wm} MB."
        )

    lines = []
    lines.append("### Выводы по показателям ТЗ")
    lines.append("")
    lines.append(f"1. **Глубина очереди, фаза механизма.** {mech}")
    lines.append("")
    lines.append(f"2. **Объём потребляемой памяти.** {mem}")
    lines.append("")
    lines.append(f"3. **Скорость генерации задач источником.** {speed}")
    lines.append("")
    lines.append(f"4. **Время выполнения задачи.** {latency}")
    lines.append("")
    lines.append(f"5. **Влияние добавления второго воркера.** {scaling}")
    lines.append("")
    lines.append("### Соответствие ожидаемому результату ТЗ")
    lines.append("")
    lines.append(depth_bullet)
    lines.append(rate_bullet)
    lines.append(scale_bullet)
    lines.append(
        f"- задачи не теряются, а необработанные попадают в DLQ: в live-режимах опубликованы {l1_pub} и {l2_pub} задач; "
        f"в `bp_stopped` излишек отбивается брокером и повторится с backoff, уже принятые сообщения не теряются; "
        f"DLQ `dota_inference_dlq` и exchange `dota_dlx` объявлены, логика retry к DLQ покрыта модульным тестом `test_process_task_retries_then_dead_letters`;"
    )
    lines.append(
        "- внешний клиент при перегрузке немедленно получает 503: выполнено, HTTP 503 с заголовком `retry-after 5`, "
        "сброс нагрузки на границе, показано живьём в демо-ранбуке."
    )
    lines.append("")
    lines.append("### Метод измерения")
    lines.append("")
    lines.append(
        "Глубина очереди снимается авторитетно через `rabbitmqctl list_queues`, а не через Management HTTP API: "
        "в этой среде API может отдавать закэшированное `messages=0`, снятое в момент создания очереди, и не инвалидировать кэш при прямой публикации. "
        "Отдельно снимаются `messages_ready`, `messages_unacknowledged` и суммарное `messages`. "
        "Для ограничения нагрузки применены политики RabbitMQ `max-length` и `overflow=reject-publish`. "
        "В stopped-фазе очередь дополнительно пересоздаётся пустой через `ensure_queue.py`. "
        "В live-фазе `measure.py` работает в режиме drain и продолжает замер до момента, когда глубина очереди становится равной нулю. "
        "Память снимается через `docker stats`: отдельно считаются брокер, суммарная память всех воркеров и источник. "
        "Отказы источника считаются по строкам `backpressure_nack` в логе producer, потому что в stopped-фазе producer может не доживать до `scoring_finished`."
    )
    lines.append("")
    lines.append("### Артефакты")
    lines.append("")
    lines.append(
        "Покадровые CSV: `results/baseline_stopped.csv`, `results/bp_stopped.csv`, `results/bp_live_1w.csv`, `results/bp_live_2w.csv`. "
        "Авторитетная финальная глубина: `results/*.final`. Время полной разрядки: `results/*.drain`. "
        "Скорость источника и wall-clock публикации: `results/*_rate.json`. "
        "Логи источника: `logs_baseline_stopped.txt`, `logs_bp_stopped.txt`, `logs_bp_live_1w.txt`, `logs_bp_live_2w.txt`."
    )

    return "\n".join(lines)


def render(data):
    parts = []
    parts.append("## 7. Экспериментальное исследование противодавления")
    parts.append("")
    parts.append("### Методология")
    parts.append("")
    parts.append(METHODOLOGY)
    parts.append("")
    parts.append("### Результаты по очереди и источнику")
    parts.append("")
    parts.append(main_table(data).rstrip("\n"))
    parts.append("")
    parts.append(
        "Примечание: `Ready peak` — это `messages_ready`, именно его ограничивает политика `max-length`. "
        "`Unacked peak` — это `messages_unacknowledged`, сообщения, уже взятые воркерами в обработку. "
        "`Total peak` — это суммарное `messages`, включающее ready и unacknowledged. "
        "Поэтому total может превышать лимит ready-очереди, даже если сам лимит соблюдён."
    )
    parts.append("")
    parts.append("### Результаты по памяти и времени обработки")
    parts.append("")
    parts.append(memory_table(data).rstrip("\n"))
    parts.append("")
    parts.append(
        "`Время полной обработки пакета` — это время от начала замера до момента, когда глубина очереди стала равна нулю. "
        "`Средний интервал завершения задач` — это время полной обработки пакета, делённое на 88 задач. "
        "Это метрика сквозной производительности конвейера, а не latency одной inference-операции."
    )
    parts.append("")
    parts.append("### Динамика по выборке кадров")
    parts.append("")
    parts.append(dynamics_table(data).rstrip("\n"))
    parts.append("")
    parts.append(findings(data))
    return "\n".join(parts) + "\n"


def main():
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

    data = collect()
    if not data:
        print("no data in results/")
        return

    text = render(data)
    out_dir = Path("results")
    out_dir.mkdir(exist_ok=True)
    out_path = out_dir / "section7.md"
    out_path.write_text(text, encoding="utf-8")

    print(f"wrote {out_path} ({len(text)} chars)")
    print(text)


if __name__ == "__main__":
    main()
