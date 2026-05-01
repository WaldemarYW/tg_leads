import argparse
import time
from zoneinfo import ZoneInfo

import auto_reply
from sheets_queue import SheetsQueueStore, calculate_backoff_sec


def extract_status_code(err: Exception):
    resp = getattr(err, "response", None)
    code = getattr(resp, "status_code", None)
    try:
        return int(code)
    except (TypeError, ValueError):
        return None


def flush_once(path: str, batch_size: int) -> int:
    tz = ZoneInfo(auto_reply.TIMEZONE)
    queue = SheetsQueueStore(path)
    sheet = auto_reply.SheetWriter()
    group_leads_sheet = auto_reply.GroupLeadsSheet()
    registration_sheet = auto_reply.RegistrationSheet()
    faq_questions_sheet = auto_reply.FAQQuestionsSheet()
    faq_suggestions_sheet = auto_reply.FAQSuggestionsSheet()

    flushed = 0
    for event in queue.fetch_batch(batch_size, time.time()):
        payload = event.payload or {}
        attempts = int(event.attempts or 0) + 1
        try:
            if event.event_type == "today_upsert":
                sheet.upsert(tz=tz, **payload)
            elif event.event_type == "group_leads_upsert":
                group_data = payload.get("data") or {}
                group_leads_sheet.upsert(tz, group_data, payload.get("status"))
                sheet.invalidate_group_leads_lookup_cache()
                sheet.refresh_today_from_group_lead(tz, group_data)
            elif event.event_type == "registration_upsert":
                registration_sheet.upsert(tz, payload)
                sheet.refresh_today_from_registration(tz, payload)
            elif event.event_type == "faq_question_log":
                faq_questions_sheet.upsert_question(payload)
                count = int(payload.get("count", 1) or 1)
                if count >= 3:
                    faq_suggestions_sheet.append_if_missing(
                        {
                            "question_cluster": payload.get("cluster_key", ""),
                            "suggested_answer": payload.get("answer_preview", ""),
                            "source_examples": payload.get("question_raw", ""),
                            "review_status": "new",
                            "reviewed_at": "",
                            "reviewed_by": "",
                        }
                    )
            elif event.event_type == "like_training_upsert":
                # Like-training writes are optional learning data; leave them for the runtime worker.
                continue
            else:
                raise ValueError(f"Unknown sheet event type: {event.event_type}")
            queue.mark_done(event.id)
            flushed += 1
            print(f"FLUSH_QUEUE_ONCE ok id={event.id} type={event.event_type}")
        except Exception as err:
            backoff = calculate_backoff_sec(attempts)
            queue.mark_retry(event.id, attempts, backoff, f"{type(err).__name__}: {err}")
            print(f"FLUSH_QUEUE_ONCE fail id={event.id} type={event.event_type} err={type(err).__name__}: {err}")
            if extract_status_code(err) == 429:
                print("FLUSH_QUEUE_ONCE quota_pause stop_batch=1")
                break
    return flushed


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("path")
    parser.add_argument("--batch-size", type=int, default=50)
    args = parser.parse_args()
    flushed = flush_once(args.path, args.batch_size)
    stats = SheetsQueueStore(args.path).stats()
    print(f"FLUSH_QUEUE_ONCE_DONE flushed={flushed} stats={stats}")


if __name__ == "__main__":
    main()
