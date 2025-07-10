import pandas as pd

def eventify():
    event_columns = {
        "ApplicationEntry": "application_entry_date",
        "Shortlist": "shortlist_date",
        "Qualification": "qualification_date",
        "ResumeSent": "resume_sent_to_company_date",
        "Interview1": "1st_interview_date",
        "Interview2": "2nd_interview_date",
        "Interview3": "3rd_interview_date",
        "Interview4": "4th_interview_date",
        "JobOfferProposed": "job_offer_proposed_date",
        "JobOfferAccepted": "job_offer_accepted_date",
        "KO": "KO_date",
    }

    df = pd.read_csv("applications.csv")

    events = []
    for event_name, col in event_columns.items():
        df_event = df[
            ["application_id", "candidate_id", "job_id", col]
        ].copy()
        df_event = df_event.rename(columns={col: "timestamp"})
        df_event["event_name"] = event_name
        df_event = df_event[df_event["timestamp"].notna()]
        events.append(df_event)


    df_events = pd.concat(events, ignore_index=True)
    df_events = df_events[
        ["application_id", "candidate_id", "job_id", "event_name", "timestamp"]
    ]

    return df_events