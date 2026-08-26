import { defineConfig } from "vitepress";

const REPO_URL = "https://github.com/TeamSpringbird/effect-temporal";

const HIRING_MAILTO =
  "mailto:ben@springbird.app" +
  "?subject=" +
  encodeURIComponent("Interested in working at Springbird") +
  "&body=" +
  encodeURIComponent(
    "Hi Ben,\n\nI found Springbird through effect-temporal and I'm interested in learning more about open roles.\n\nA bit about me:\n\n",
  );
const DESCRIPTION =
  "Durable Effect workflows on Temporal. Schema-typed payloads, typed failures, and Temporal's operational muscle in one engine.";

export default defineConfig({
  lang: "en-US",
  title: "effect-temporal",
  description: DESCRIPTION,
  // Set by the Pages deploy workflow; with the custom domain
  // (www.effect-temporal.com) the site serves at the root.
  base: process.env.DOCS_BASE || "/",
  cleanUrls: true,
  lastUpdated: true,
  sitemap: { hostname: "https://www.effect-temporal.com" },
  head: [
    // Springbird brand color and webfonts (Poppins body, Plus Jakarta Sans
    // display — the marketing site's pairing).
    ["meta", { name: "theme-color", content: "#47c4cd" }],
    ["link", { rel: "preconnect", href: "https://fonts.googleapis.com" }],
    ["link", { rel: "preconnect", href: "https://fonts.gstatic.com", crossorigin: "" }],
    [
      "link",
      {
        rel: "stylesheet",
        href: "https://fonts.googleapis.com/css2?family=Poppins:wght@300;400;500;600;700&family=Plus+Jakarta+Sans:wght@400..800&display=swap",
      },
    ],
  ],
  // Per-page share cards: og:title/og:description are per page, so they live
  // here instead of the static head.
  transformPageData(pageData) {
    const title =
      pageData.title === "" || pageData.title === "effect-temporal"
        ? "effect-temporal"
        : `${pageData.title} · effect-temporal`;
    const description = pageData.description === "" ? DESCRIPTION : pageData.description;
    pageData.frontmatter.head ??= [];
    pageData.frontmatter.head.push(
      ["meta", { property: "og:title", content: title }],
      ["meta", { property: "og:description", content: description }],
    );
  },
  markdown: {
    theme: { light: "github-light", dark: "github-dark" },
  },
  themeConfig: {
    siteTitle: "effect-temporal",
    search: { provider: "local" },
    nav: [
      { text: "Guide", link: "/guide/getting-started", activeMatch: "/guide/" },
      { text: "Reference", link: "/reference/how-it-works", activeMatch: "/reference/" },
      { text: "npm", link: "https://www.npmjs.com/package/@springbird/effect-temporal" },
    ],
    sidebar: [
      {
        text: "Start",
        items: [
          { text: "What is effect-temporal?", link: "/guide/introduction" },
          { text: "Getting started", link: "/guide/getting-started" },
        ],
      },
      {
        text: "Author",
        items: [
          { text: "Defining workflows", link: "/guide/defining-workflows" },
          { text: "Activities", link: "/guide/activities" },
          { text: "Timers & approvals", link: "/guide/timers-and-approvals" },
          { text: "Child workflows", link: "/guide/child-workflows" },
          { text: "Continue-as-new", link: "/guide/continue-as-new" },
          { text: "Versioning", link: "/guide/versioning" },
        ],
      },
      {
        text: "Communicate",
        items: [
          { text: "Mailboxes", link: "/guide/mailboxes" },
          { text: "Updates", link: "/guide/updates" },
          { text: "Queryable state", link: "/guide/queryable-state" },
        ],
      },
      {
        text: "Operate",
        items: [
          { text: "Cancellation & compensation", link: "/guide/cancellation" },
          { text: "Schedules", link: "/guide/schedules" },
          { text: "Nexus operations", link: "/guide/nexus" },
          { text: "Testing your app", link: "/guide/testing" },
          { text: "Lint rules", link: "/guide/lint-rules" },
        ],
      },
      {
        text: "Reference",
        items: [
          { text: "How the engine works", link: "/reference/how-it-works" },
          { text: "Data conversion", link: "/reference/data-conversion" },
          { text: "Limitations", link: "/reference/limitations" },
        ],
      },
    ],
    socialLinks: [{ icon: "github", link: REPO_URL }],
    editLink: {
      pattern: `${REPO_URL}/edit/main/docs/:path`,
      text: "Edit this page",
    },
    outline: { level: [2, 3] },
    footer: {
      message:
        'Released under the MIT License.<br>Made with ❤️ by <a href="https://springbird.app">Springbird</a> (<a href="' +
        HIRING_MAILTO +
        '">We\'re hiring</a>).',
    },
  },
});
