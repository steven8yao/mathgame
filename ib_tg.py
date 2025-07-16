import asyncio
import nest_asyncio
from telegram import Bot
import os # For path joining
import logging # For logging levels
user_home_directory = os.path.expanduser('~')
project_root_parent = os.path.join(user_home_directory, 'GitHub')

# Import from your app_logger
from ib_logger import create_file_logger # Assuming app_logger.py is in pairstrading package

try:
    from config import HOUSE_BOT_TOKEN, MY_CHANNEL_ID
    MY_BOT_TOKEN = HOUSE_BOT_TOKEN
except ImportError as e:
    # This initial print will go to console as dedicated logger isn't set up yet
    print(f"[ERROR][ib_telegram.py] Error importing pairstrading.config: {e}")
    MY_BOT_TOKEN = None
    MY_CHANNEL_ID = None

telegram_log_file_path = os.path.join(project_root_parent, 'calgary_house.log')

# Ensure the directory for telegram.log exists
os.makedirs(os.path.dirname(telegram_log_file_path), exist_ok=True)

# Create the dedicated logger for telegram.log
telegram_dedicated_logger = create_file_logger(
    logger_name='telegram_dedicated_logger',
    log_file_full_path=telegram_log_file_path,
    level=logging.INFO # Or logging.DEBUG
)
telegram_dedicated_logger.info("--- ib_telegram.py module loaded, telegram_dedicated_logger initialized ---")

# --- Telegram Bot Initialization ---
bot = None
if MY_BOT_TOKEN:
    try:
        bot = Bot(token=MY_BOT_TOKEN)
        nest_asyncio.apply() # Apply nest_asyncio if bot is initialized
        telegram_dedicated_logger.info("Telegram bot initialized and nest_asyncio applied.")
    except Exception as e:
        telegram_dedicated_logger.error(f"Failed to initialize Telegram bot: {e}", exc_info=True)
        bot = None
else:
    telegram_dedicated_logger.warning("Telegram BOT_TOKEN is not configured. Telegram features will be disabled.")


def send_telegram_message(message: str, parse_mode: str = 'HTML', channel_id: str = None, caller_logger=None):
    """
    Sends a message to the specified Telegram channel.
    Logs actions to telegram.log and also using the provided caller_logger.

    :param message: The message text.
    :param parse_mode: Parse mode for the message (e.g., 'HTML', 'MarkdownV2').
    :param channel_id: The target channel ID. Defaults to MY_CHANNEL_ID from config.
    :param caller_logger: An instance of LogPrintHelper (or similar) from the calling script.
    """
    if not bot:
        log_message = "Telegram bot is not initialized or failed to initialize. Cannot send message."
        telegram_dedicated_logger.error(log_message)
        if caller_logger:
            caller_logger.error(log_message)
        else:
            print(f"[ERROR] {log_message} (No caller_logger provided to send_telegram_message)")
        return

    target_channel_id = channel_id if channel_id is not None else MY_CHANNEL_ID

    if not target_channel_id:
        log_message = "Target Telegram channel_id is not configured or provided. Cannot send message."
        telegram_dedicated_logger.error(log_message)
        if caller_logger:
            caller_logger.error(log_message)
        else:
            print(f"[ERROR] {log_message} (No caller_logger provided to send_telegram_message)")
        return

    try:
        loop = asyncio.get_event_loop()
        if loop.is_closed():
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)

        if loop.is_running():
            asyncio.create_task(bot.send_message(chat_id=target_channel_id, text=message, parse_mode=parse_mode))
        else:
            loop.run_until_complete(bot.send_message(chat_id=target_channel_id, text=message, parse_mode=parse_mode))
        
        log_message_success = f"Telegram message sent to {target_channel_id}. Content: {message[:70]}..."
        telegram_dedicated_logger.info(log_message_success)
        if caller_logger:
            caller_logger.info(log_message_success)
        # No else print here for success to avoid too much console noise if caller_logger is not used

    except RuntimeError as e:
        log_message_error = f"RuntimeError sending Telegram to {target_channel_id} (event loop issue): {e}"
        if "cannot schedule new futures after shutdown" in str(e) or "Event loop is closed" in str(e):
            telegram_dedicated_logger.warning(log_message_error + ". Trying with new/temp loop.")
            if caller_logger: caller_logger.warning(log_message_error + ". Trying with new/temp loop.")
            try:
                asyncio.run(bot.send_message(chat_id=target_channel_id, text=message, parse_mode=parse_mode))
                log_message_success_retry = f"Telegram message sent to {target_channel_id} after retry. Content: {message[:70]}..."
                telegram_dedicated_logger.info(log_message_success_retry)
                if caller_logger: caller_logger.info(log_message_success_retry)
            except Exception as ex_new_loop:
                log_message_error_retry = f"Failed to send to Telegram {target_channel_id} even with new loop: {ex_new_loop}"
                telegram_dedicated_logger.error(log_message_error_retry, exc_info=True)
                if caller_logger: caller_logger.error(log_message_error_retry, exc_info=True)
                else: print(f"[ERROR] {log_message_error_retry} (No caller_logger)")
        else:
            telegram_dedicated_logger.error(log_message_error, exc_info=True)
            if caller_logger: caller_logger.error(log_message_error, exc_info=True)
            else: print(f"[ERROR] {log_message_error} (No caller_logger)")
    except Exception as e:
        log_message_error = f"Failed to send message to Telegram {target_channel_id}: {e}"
        telegram_dedicated_logger.error(log_message_error, exc_info=True)
        if caller_logger:
            caller_logger.error(log_message_error, exc_info=True)
        else:
            print(f"[ERROR] {log_message_error} (No caller_logger provided to send_telegram_message)")

if __name__ == '__main__':
    # For direct testing of ib_telegram.py
    print(f"ib_telegram.py direct test. Dedicated Telegram logs will go to: {telegram_log_file_path}")

    try:
        from ib_logger import LogPrintHelper # create_file_logger already imported
        
        temp_caller_log_file = 'ib_telegram_caller_test.log'
        temp_caller_file_logger = create_file_logger('ib_telegram_temp_caller_logger', temp_caller_log_file)
        test_caller_log_print = LogPrintHelper(temp_caller_file_logger, print_to_console=True)
        
        test_caller_log_print.info("Direct test of ib_telegram.py started (simulating caller).")
        if bot and MY_CHANNEL_ID:
            send_telegram_message(
                "<b>Test HTML Message</b> to <i>default channel</i> from <code>ib_telegram.py</code> direct run with caller_logger.",
                parse_mode='HTML',
                caller_logger=test_caller_log_print
            )
        else:
            test_caller_log_print.warning("Telegram bot not initialized or MY_CHANNEL_ID not set. Skipping test send.")
        test_caller_log_print.info("Direct test of ib_telegram.py finished (simulating caller).")
        print(f"Check console, {telegram_log_file_path}, and {temp_caller_log_file}")

    except ImportError:
        print("Could not import app_logger for full ib_telegram.py direct test. Running with basic console fallback for caller_logger.")
        if bot and MY_CHANNEL_ID:
            send_telegram_message(
                "Test Plain Message from ib_telegram.py (console fallback for caller_logger).",
                parse_mode=None
            )