import os
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes

from dq_runner import run_validation
from dotenv import load_dotenv
load_dotenv()
BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")




async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "Hi 👋\n"
        "Commands:\n"
        "/dq - run Great Expectations data quality checks and get report"
    )


async def dq(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("🔍 Running Great Expectations checks...")

    res = run_validation()

    msg = (
        f"📊 Data Quality Report\n"
        f"✅ Success: {res['success']}\n"
        f"Total: {res['total']}\n"
        f"Passed: {res['passed']}\n"
        f"Failed: {res['failed']}\n"
    )
    await update.message.reply_text(msg)

    # send HTML report
    with open(res["html_path"], "rb") as f:
        await update.message.reply_document(
            document=f,
            filename=os.path.basename(res["html_path"])
        )


def main():
    if not BOT_TOKEN:
        raise ValueError("Missing env var TELEGRAM_BOT_TOKEN")

    app = ApplicationBuilder().token(BOT_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("dq", dq))

    app.run_polling()


if __name__ == "__main__":
    main()
