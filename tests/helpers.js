const USERNAME = process.env.SP_USER || 'admin';
const PASSWORD = process.env.SP_PASS || 'stockpicker2024';

async function login(page) {
  await page.goto('/');
  // Wait for page to load
  await page.waitForLoadState('domcontentloaded');

  const overlay = page.locator('#login-overlay');
  const isVisible = await overlay.evaluate(el => el.style.display !== 'none').catch(() => true);

  if (isVisible) {
    await page.fill('#login-username', USERNAME);
    await page.fill('#login-password', PASSWORD);
    await page.click('#btn-login');
    // Wait for the overlay to be hidden
    await page.waitForFunction(
      () => {
        const el = document.getElementById('login-overlay');
        if (!el) return true;
        return el.style.display === 'none' || getComputedStyle(el).display === 'none';
      },
      { timeout: 20000 }
    );
  }
}

module.exports = { login, USERNAME, PASSWORD };
