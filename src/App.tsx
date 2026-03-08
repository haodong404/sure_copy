import { createResource, createSignal, useTransition } from "solid-js";
import "./App.css";
import * as i18n from "@solid-primitives/i18n";
import { invoke } from "@tauri-apps/api/core";
import logo from "./assets/logo.svg";

async function fetchDictionary(locale: Locale): Promise<Dictionary> {
  const dict = await import(`./i18n/${locale}.json`);
  return i18n.flatten(dict); // flatten the dictionary to make all nested keys available top-level
}

function App() {
  const [greetMsg, setGreetMsg] = createSignal("");
  const [name, setName] = createSignal("");

  async function greet() {
    // Learn more about Tauri commands at https://tauri.app/develop/calling-rust/
    setGreetMsg(await invoke("greet", { name: name() }));
    switchLocale(locale() === "en" ? "zh" : "en");
  }
  const [locale, setLocale] = createSignal<Locale>("en");

  const [dict] = createResource<Dictionary>(locale, fetchDictionary);

  const [duringTransition, startTransition] = useTransition();

  function switchLocale(locale: Locale) {
    startTransition(() => setLocale(locale));
  }

  const t = i18n.translator(dict);

  return (
    <main class="container">
      <h1>Welcome to Tauri + Solid</h1>

      <div class="row">
        <a href="https://vite.dev" target="_blank" rel="noopener">
          <img src="/vite.svg" class="logo vite" alt="Vite logo" />
        </a>
        <a href="https://tauri.app" target="_blank" rel="noopener">
          <img src="/tauri.svg" class="logo tauri" alt="Tauri logo" />
        </a>
        <a href="https://solidjs.com" target="_blank" rel="noopener">
          <img src={logo} class="logo solid" alt="Solid logo" />
        </a>
      </div>
      <p>Click on the Tauri, Vite, and Solid logos to learn more.</p>
      <p>{t("greet.morning") ?? ""}</p>
      <form
        class="row"
        onSubmit={(e) => {
          e.preventDefault();
          greet();
        }}
        style={{
          opacity: duringTransition() ? "0.5" : "1",
        }}
      >
        <input
          id="greet-input"
          name="greet-input"
          onChange={(e) => setName(e.currentTarget.value)}
          placeholder="Enter a name..."
        />
        <button type="submit">Greet</button>
      </form>
      <p>{greetMsg()}</p>
    </main>
  );
}

export default App;
