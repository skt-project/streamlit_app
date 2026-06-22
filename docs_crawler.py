"""Documentation screenshot crawler — Area Sales Supervisor workflow only.

Logs in as budi/sales123, walks through every screen/state reachable by the
Area Sales Supervisor role, and saves full-page screenshots to ./screenshots
for use in User_Guide.md / User_Guide.html / User_Training.pptx.

Run against the MOCK app (skt_area_execution_capability_mock.py) — see
USER_GUIDE generation notes for why (no live BigQuery credentials available,
and the mock is visually identical for documentation purposes while being
safe to click through repeatedly without writing real production data).

Usage: python docs_crawler.py   (app must already be running on localhost:8501)
"""
from pathlib import Path
from playwright.sync_api import sync_playwright

SCREENSHOTS_DIR = Path(__file__).parent / "screenshots"
SCREENSHOTS_DIR.mkdir(exist_ok=True)
BASE_URL = "http://localhost:8501"


def shot(page, name, clip=None, full_page=True):
    path = SCREENSHOTS_DIR / f"{name}.png"
    if clip:
        page.screenshot(path=str(path), clip=clip)
    else:
        page.screenshot(path=str(path), full_page=full_page)
    print(f"  saved {path.name}")


def main():
    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page(viewport={"width": 1440, "height": 1000})

        print("01-02: Login screen")
        page.goto(BASE_URL, wait_until="networkidle", timeout=20000)
        page.wait_for_timeout(1200)
        shot(page, "01_login_screen", full_page=False)

        page.get_by_placeholder("e.g. budi").fill("budi")
        page.get_by_placeholder("••••••••").fill("sales123")
        shot(page, "02_login_filled", full_page=False)
        page.get_by_role("button", name="Log In").click()
        page.wait_for_timeout(1200)

        print("03: Post-login, form locked (no distributor selected yet)")
        shot(page, "03_post_login_locked", full_page=False)

        print("04: Assessment Period dropdown open")
        page.get_by_role("combobox", name="Assessment Period").click()
        page.wait_for_timeout(400)
        shot(page, "04_assessment_period_dropdown", full_page=False)
        page.keyboard.press("Escape")
        page.wait_for_timeout(300)

        print("05: Region locked + Distributor dropdown open")
        page.get_by_role("combobox", name="Distributor").click()
        page.wait_for_timeout(400)
        shot(page, "05_distributor_dropdown", full_page=False)
        page.get_by_role("option", name="CV Maju Bersama - Jakarta Pusat").click()
        page.wait_for_timeout(800)

        print("06: Form unlocked, top of People & Roles category")
        shot(page, "06_form_unlocked_people_roles")

        print("07: Fill People & Roles fields")
        page.locator('input[aria-label="Name (required if role exists)"]').nth(0).fill("Andi Wijaya")
        for i in range(1, 6):
            page.locator(f'input[aria-label="Name {i}"]').fill(f"Salesman {i}")
        page.locator('input[aria-label="Name (required if role exists)"]').nth(1).fill("Citra Dewi")
        page.wait_for_timeout(300)
        shot(page, "07_people_roles_filled")

        print("08: Scroll to Infrastructure & Delivery (Warehouse + Delivery SLA)")
        page.get_by_text("WAREHOUSE FACILITY STANDARD").scroll_into_view_if_needed()
        page.wait_for_timeout(400)
        shot(page, "08_infrastructure_delivery", full_page=False)

        print("09: Scroll to Operations & Compliance (Inventory + Bad Stock)")
        page.get_by_text("INVENTORY CONTROL & STOCK OPNAME").scroll_into_view_if_needed()
        page.wait_for_timeout(400)
        shot(page, "09_operations_compliance", full_page=False)

        print("10: Bad Stock card close-up with utilization filled")
        util_input = page.locator('input[aria-label="Bad Stock Utilization (Rp)"]')
        util_input.fill("5000000")
        util_input.press("Tab")
        page.wait_for_timeout(600)
        page.get_by_text("BAD STOCK HANDLING PERFORMANCE").scroll_into_view_if_needed()
        page.wait_for_timeout(300)
        shot(page, "10_bad_stock_detail", full_page=False)

        print("11: Sidebar — live score tracker + assessment progress")
        page.mouse.move(700, 400)
        page.mouse.wheel(0, -10000)
        page.wait_for_timeout(400)
        shot(page, "11_sidebar_score_tracker", clip={"x": 0, "y": 0, "width": 300, "height": 1000})

        print("12: Click Review & Submit -> confirmation popup")
        page.get_by_role("button", name="Review & Submit").click()
        page.wait_for_timeout(1000)
        shot(page, "12_review_submit_popup", full_page=False)

        print("13: Confirm & Submit -> result / completion view")
        page.get_by_role("button", name="Confirm & Submit").click()
        page.wait_for_timeout(2000)
        page.mouse.move(700, 400)
        page.mouse.wheel(0, -10000)
        page.wait_for_timeout(400)
        shot(page, "13_submission_result", full_page=False)

        print("14: Change Password panel")
        page.get_by_text("Change Password").click()
        page.wait_for_timeout(500)
        shot(page, "14_change_password", clip={"x": 0, "y": 0, "width": 300, "height": 500})

        browser.close()
    print("\nDone. Screenshots in:", SCREENSHOTS_DIR)


if __name__ == "__main__":
    main()
