import asyncio
from playwright.async_api import async_playwright

async def main():
    print("Starting playwright...")
    async with async_playwright() as p:
        print("Launching browser (webkit)...")
        try:
            browser = await p.webkit.launch(headless=True)
            print("Browser launched!")
            page = await browser.new_page()
            print("Page created!")
            await browser.close()
            print("Browser closed!")
        except Exception as e:
            print(f"Error: {e}")

if __name__ == "__main__":
    asyncio.run(main())
