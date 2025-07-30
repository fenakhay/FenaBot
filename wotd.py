import pywikibot
import re
import sys
import threading
import calendar
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

START_YEAR = 2006
START_MONTH = 1
END_YEAR = 2011
END_MONTH = 12

DEFAULT_DRY_RUN = False

print_lock = threading.Lock()

def process_one_day(task_info):
    year = task_info['year']
    month_name = task_info['month_name']
    day = task_info['day']
    content = task_info['content']
    site = task_info['site']
    dry_run = task_info['dry_run']
    create_summary = task_info['create_summary']
    protect_reason = task_info['protect_reason']

    target_page_title = f"Wiktionary:Word of the day/{year}/{month_name} {day}"

    with print_lock:
        pywikibot.output(f"\n--- Day {day} (Thread: {threading.current_thread().name}) ---")
        pywikibot.output(f"Target page: {target_page_title}")

    target_page = pywikibot.Page(site, target_page_title)
    page_exists = target_page.exists()

    if not page_exists:
        with print_lock:
            pywikibot.output("Content to be saved:")
            pywikibot.output("-" * 25)
            pywikibot.output(content)
            pywikibot.output("-" * 25)

        if not dry_run:
            try:
                target_page.text = content
                target_page.save(summary=create_summary)
                with print_lock:
                    pywikibot.output("SUCCESS: New page created.")
            except pywikibot.Error as e:
                with print_lock:
                    pywikibot.error(f"ERROR: Failed to create page. Reason: {e}")
                return
    else:
        with print_lock:
            pywikibot.output("INFO: Page already exists, skipping creation.")

    if not dry_run:
        try:
            # Check current protection to avoid redundant edits
            current_protection = target_page.protection()
            if current_protection.get('edit', (None,))[0] != 'sysop':
                protections = {'edit': 'sysop', 'move': 'sysop'}
                target_page.protect(protections=protections, reason=protect_reason)
                with print_lock:
                    pywikibot.output("SUCCESS: Page has been protected.")
            else:
                with print_lock:
                    pywikibot.output("INFO: Page is already correctly protected.")
        except pywikibot.Error as e:
            with print_lock:
                pywikibot.error(f"ERROR: Failed to protect page. Reason: {e}")
    else:
        with print_lock:
            pywikibot.output("DRY RUN: Page would be checked and protected if necessary.")


def run_wotd_processing_for_month(site, year, month_name, dry_run=True):
    create_summary = f"Creating Word of the Day page from the {month_name} {year} archive."
    protect_reason = "Protecting created Word of the Day page."
    archive_summary = f"Archiving WOTD source page for {month_name} {year}."

    source_page_title = f"Wiktionary:Word of the day/Archive/{year}/{month_name}"
    source_page = pywikibot.Page(site, source_page_title)

    if not source_page.exists():
        with print_lock:
            pywikibot.error(f"Source page '{source_page_title}' does not exist. Skipping month.")
        return

    wikitext = source_page.text
    sections = re.split(r'==\s*(\d+)\s*==', wikitext)

    tasks = []
    was_section_skipped = False

    for i in range(1, len(sections), 2):
        day = sections[i]
        content = sections[i + 1].strip()

        if content and '{{wotd' in content.lower():
            tasks.append({
                'year': year, 'month_name': month_name, 'day': day, 'content': content,
                'site': site, 'dry_run': dry_run, 'create_summary': create_summary,
                'protect_reason': protect_reason,
            })
        elif content:
            was_section_skipped = True
            with print_lock:
                pywikibot.warning(f"Skipping section for day {day} as it does not contain a WOTD template.")

    if not tasks:
        with print_lock:
            pywikibot.warning(f"No valid WOTD sections found in '{source_page_title}'. Skipping.")
        return

    num_workers = len(tasks)

    with print_lock:
        pywikibot.output(
            f"\nStarting processing for {num_workers} days in {month_name} {year} using {num_workers} threads...")

    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        executor.map(process_one_day, tasks)

    with print_lock:
        pywikibot.output(f"\nConcurrent tasks complete for {month_name} {year}.")

    if was_section_skipped:
        with print_lock:
            pywikibot.error(
                f"\nSource page for {month_name} {year} will NOT be archived because one or more sections were invalid.")
    else:
        new_content = f"{{{{WOTD month archive|{year}|{{{{SUBPAGENAME}}}}}}}}"
        if not dry_run:
            try:
                source_page.text = new_content
                source_page.save(summary=archive_summary)
                with print_lock:
                    pywikibot.output(f"SUCCESS: Source page for {month_name} {year} has been archived.")
            except pywikibot.Error as e:
                with print_lock:
                    pywikibot.error(f"Could not update source page for {month_name} {year}. Reason: {e}")
        else:
            with print_lock:
                pywikibot.output(
                    f"\nDRY RUN: All sections valid. Would replace content of '{source_page_title}' with: {new_content}")


def main():
    is_dry_run = DEFAULT_DRY_RUN
    if '-live' in sys.argv:
        is_dry_run = False
        pywikibot.output("\n*** LIVE MODE: Edits will be saved to Wiktionary! ***\n")
        sys.argv.remove('-live')
    else:
        pywikibot.output("\n*** DRY RUN MODE: No edits will be made. Use -live argument to save. ***\n")

    site = pywikibot.Site('en', 'wiktionary')
    site.login()

    now = datetime.now()

    for year in range(START_YEAR, END_YEAR + 1):
        month_start_num = START_MONTH if year == START_YEAR else 1
        month_end_num = END_MONTH if year == END_YEAR else 12

        for month_num in range(month_start_num, month_end_num + 1):
            if year > now.year or (year == now.year and month_num > now.month):
                pywikibot.output("\n" + "#" * 50)
                pywikibot.output(
                    f"### Reached future date ({calendar.month_name[month_num]} {year}). Stopping script. ###")
                pywikibot.output("#" * 50)
                return

            month_name = calendar.month_name[month_num]
            pywikibot.output("\n" + "#" * 50)
            pywikibot.output(f"### PROCESSING {month_name.upper()} {year} ###")
            pywikibot.output("#" * 50)
            run_wotd_processing_for_month(site, year, month_name, dry_run=is_dry_run)

    pywikibot.output("\nScript finished.")


if __name__ == "__main__":
    main()