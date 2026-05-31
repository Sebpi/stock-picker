const USERNAME = process.env.SP_USER || 'admin';
const PASSWORD = process.env.SP_PASS || 'stockpicker2024';

async function login(page) {
  await page.goto('/');
  await page.waitForLoadState('domcontentloaded');

  // Wait for React to render — either the login form or the main app
  await page.waitForFunction(
    () => document.getElementById('login-overlay') || document.querySelector('button[data-tab]'),
    { timeout: 15000 }
  );

  const loginForm = page.locator('#login-overlay');
  const isLoginVisible = await loginForm.isVisible().catch(() => false);

  if (isLoginVisible) {
    await page.fill('#login-username', USERNAME);
    await page.fill('#login-password', PASSWORD);
    await page.click('#btn-login');
    // Wait for the login overlay to disappear (React unmounts it on success)
    await page.waitForFunction(
      () => !document.getElementById('login-overlay'),
      { timeout: 20000 }
    );
  }
}

module.exports = { login, USERNAME, PASSWORD };
