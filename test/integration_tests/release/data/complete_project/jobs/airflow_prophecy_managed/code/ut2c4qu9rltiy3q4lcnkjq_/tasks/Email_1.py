def Email_1():
    settings = {}
    from airflow.operators.email import EmailOperator
    from datetime import timedelta

    return EmailOperator(
        task_id = "Email_1",
        to = "pankaj@prophecy.io",
        subject = "Delta",
        html_content = "Delta",
        cc = None,
        bcc = None,
        mime_subtype = "mixed",
        mime_charset = "utf-8",
        conn_id = "ATSPdLpyCoWns1X5aXZeO",
        **settings
    )
