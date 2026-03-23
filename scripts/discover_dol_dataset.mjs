import { chromium } from 'playwright';

const override = process.env.DATASET_URL_OVERRIDE?.trim();
if (override) {
  console.log(override);
  process.exit(0);
}

const preferredYear = String(process.env.PREFERRED_YEAR || '2024');
const pageUrl = 'https://www.dol.gov/agencies/ebsa/about-ebsa/our-activities/public-disclosure/foia/form-5500-datasets';

const browser = await chromium.launch({ headless: true });
const page = await browser.newPage();
await page.goto(pageUrl, { waitUntil: 'domcontentloaded', timeout: 120000 });
await page.waitForTimeout(5000);

const links = await page.$$eval('a[href]', anchors => anchors.map(a => ({
  href: a.href,
  text: (a.textContent || '').trim(),
  aria: (a.getAttribute('aria-label') || '').trim(),
  title: (a.getAttribute('title') || '').trim(),
})));

const candidates = links
  .filter(l => /\.zip(\?|$)/i.test(l.href) || /zip/i.test(`${l.text} ${l.aria} ${l.title}`))
  .map(l => ({ ...l, haystack: `${l.href} ${l.text} ${l.aria} ${l.title}` }))
  .filter(l => /5500|form/i.test(l.haystack));

if (!candidates.length) {
  console.error('No candidate ZIP links found on DOL page.');
  process.exit(1);
}

const scored = candidates.map(l => {
  let score = 0;
  if (new RegExp(preferredYear).test(l.haystack)) score += 50;
  if (/latest/i.test(l.haystack)) score += 20;
  if (/annual|dataset/i.test(l.haystack)) score += 10;
  if (/archives?/i.test(l.haystack)) score -= 10;
  return { ...l, score };
}).sort((a, b) => b.score - a.score);

console.log(scored[0].href);
await browser.close();
