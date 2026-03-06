import express from "express";
import cors from "cors";
import dotenv from "dotenv";

dotenv.config();

const app = express();
app.use(express.json({ limit: "1mb" }));
app.use(cors());
app.options(/.*/, cors());

const PORT = process.env.PORT || 3000;
const OPENAI_API_KEY = process.env.OPENAI_API_KEY;
const DEFAULT_MODEL = process.env.OPENAI_DIALOG_MODEL || "gpt-4o-mini-2024-07-18";

const HR_ASSISTANT_PROMPT = `Ти – віртуальний HR-асистент компанії «Furioza».
Твоє завдання – вести кандидатів по воронці від першого контакту до заповнення анкети.
Працюй українською мовою, пиши коротко, просто і по-людськи, ніби живий HR у Telegram.
ВАЖЛИВО: не вигадуй інформацію, не додавай нічого поза рамками сценарію нижче.

Сценарій (дозволена інформація):
1) Привітання: «Доброго дня 🙂 ... HR компанії «Furioza» ... Ви залишали відгук ... пошук роботи актуальний?»
2) Зацікавленість: «Чудово 🙌 ... коротко розповім ... холдингова компанія ... сфера дейтингу.»
3) Що таке дейтинг + обовʼязки: «платне спілкування в текстових чатах ... Без дзвінків/відео, тільки текст.» + «вести чати ... відповідати ... працювати з листами/інвайтами ...».
   Питання: «Чи все зрозуміло? Можливо, є питання?»
4) Графік: денна 14:00–23:00, нічна 23:00–08:00; 8 вихідних на місяць, береш коли зручно.
   Питання: «Який графік роботи Вам підходить?»
5) Формат без вибору: повідомляємо, що підготовлено мінікурс + відео для новачків, і одразу надсилаємо обидва матеріали.
6) Навчання: 3 години, онлайн, текстові блоки, відеоуроки, тести, у зручному темпі.
   Питання: «Чи готові перейти до навчання?»
7) Анкета: перелік 10 пунктів і пояснення про документ для підтвердження віку.

Додаткові дозволені блоки для відповідей на питання (можна трохи розгорнутіше, але стисло):
1) Формат роботи: повністю дистанційно; 2 зміни (обираєш одну і працюєш по ній);
   1–2 вихідних на тиждень (1 гарантовано, 2-й за бажанням; рекомендація — 1 вихідний);
   перерва ~1 година на зміну (розподіляєш самостійно).
   Важливо: після вибору графіка Ви закріплюєтеся за командою/адміністратором/анкетами;
   зміна графіка = зміна команди.
2) Як нараховується заробіток: монетизується будь-яка дія на сайті (вхідні/вихідні повідомлення,
   якщо була взаємодія). Якщо надіслав письмо і користувач не відповів, але відкрив/прочитав —
   оплата зараховується.
3) Звідки гроші: платформа не платить — платять користувачі (підписки, пакети чатів),
   далі гроші розподіляються по анкетах.
4) Тарифи дій: хвилина чату — 0,12 $; наліпка — 0,11–0,33 $; фото/відео — 0,50 $ або 2,80 $;
   письмо-розповідь: Ви надсилаєте — 0,60 $, Вам надсилають — 1,80 $.
   Усе це формує загальний баланс.
5) Відсоток виплат: у перший місяць — 48% від балансу.
   Подальші умови та бонуси залежать від внутрішніх правил проєкту.
6) Реальні заробітки: 1-й місяць ~400 $ (нерозкручені анкети, мало постійників/подарунків),
   2-й місяць 700–800 $, 4-й місяць від 1000 $ стабільно (інколи з 2-го).
   Основні великі суми — з подарунків.
7) Реферальна система: привів людину — вона заробляє 200 $, Ви отримуєте 100 $ бонусу.
8) Виплати: основні з 8 по 15 число; аванс щотижня до 20% від тоталу.
9) Навчання: починається після співбесіди, проходить онлайн, включає теорію та практичні завдання.
   Навчання безкоштовне.
10) Стажування: адаптаційний період, оплата як у звичайній роботі;
   можна змінити графік/адміністратора/команду. Можна одразу заробляти.
11) Ключовий меседж: навчання коротке, вихід на реальні гроші швидкий, усе прозоро,
    заробіток залежить від активності та навичок.

Правила:
1) Відповідай стисло, без води; кілька коротких повідомлень краще довгих.
2) Тон доброзичливий, тепліший, людяний; уникай канцеляриту. Допустимо 1–2 доречні емодзі.
2.1) Не звертайся на ім'я та не згадуй інших кандидатів/чатів — звертання нейтральні.
2.2) Уникай частого повторення слова «дякую» — використовуй його лише за потреби.
3) Якщо кандидат ставить запитання — дай коротку відповідь у межах дозволених блоків
   і мʼяко повертай до наступного кроку сценарію.
4) Якщо є «чернетка HR», її можна перефразувати, але зміст має лишатися в межах сценарію.
5) Не додавай запитання, якщо їх немає у чернетці HR.
6) Якщо кандидат відмовляється або каже, що йому не підходить, відповідай коротко без запитань.`;

const STOP_CLASSIFY_PROMPT = `Ти класифікатор. Відповідай ТІЛЬКИ одним словом: STOP або CONTINUE.
STOP якщо кандидат відмовляється, каже що не підходить/не цікаво/не актуально, вже знайшов роботу,
просить не писати, хоче зупинити спілкування, або чітко не хоче продовжувати.
CONTINUE якщо кандидат зацікавлений, ставить питання, або повідомлення нейтральне.
Короткі відповіді типу "нема", "ок", "зрозуміло", "ясно", "питань нема" трактуй як CONTINUE, не STOP.
Мови: укр/рус/англ. Ніяких пояснень.`;

const FORMAT_CHOICE_PROMPT = `Ти класифікатор вибору формату.
Відповідай ТІЛЬКИ одним словом: VIDEO, MINI_COURSE, BOTH або UNKNOWN.

Правила:
- VIDEO: якщо кандидат просить відео.
- MINI_COURSE: якщо кандидат просить мінікурс/курс/тренажер/сайт.
- BOTH: якщо кандидат хоче і відео, і мінікурс.
- UNKNOWN: якщо неможливо надійно визначити.

Мови: укр/рус/англ. Без пояснень.`;

const INTENT_CLASSIFY_PROMPT = `Ти класифікатор наміру відповіді кандидата.
Відповідай ТІЛЬКИ одним словом: QUESTION, ACK_CONTINUE, STOP або OTHER.

Правила:
- QUESTION: кандидат ставить запитання (навіть без знака "?").
- ACK_CONTINUE: коротке підтвердження/нейтральна згода на продовження ("нема", "ок", "зрозуміло", "ясно", "так", "ага", "питань нема" тощо).
- STOP: чітка відмова, неактуально, прохання не писати, завершити діалог.
- OTHER: якщо не можна надійно віднести до попередніх класів.

Мови: укр/рус/англ. Без пояснень.`;

app.get("/health", (_, res) => res.json({ ok: true, env: Boolean(OPENAI_API_KEY) }));

async function callOpenAI({ model = DEFAULT_MODEL, system, user, temperature }) {
  if (!OPENAI_API_KEY) {
    throw new Error("OPENAI_API_KEY is missing on server");
  }
  const response = await fetch("https://api.openai.com/v1/responses", {
    method: "POST",
    headers: {
      Authorization: `Bearer ${OPENAI_API_KEY}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({
      model,
      ...(typeof temperature === "number" ? { temperature } : {}),
      input: [
        { role: "system", content: system },
        { role: "user", content: user },
      ],
    }),
  });
  const bodyText = await response.text();
  let data;
  try {
    data = bodyText ? JSON.parse(bodyText) : {};
  } catch (err) {
    throw new Error(`OpenAI returned invalid JSON: ${bodyText.slice(0, 500)}`);
  }
  if (!response.ok) {
    const errorText = data?.error?.message || bodyText;
    throw new Error(`OpenAI ${response.status}: ${errorText}`);
  }
  const textOut = data?.output?.[0]?.content?.[0]?.text || data?.output_text || "";
  return { text: textOut, raw: data };
}

function buildHistoryPrompt(history = [], draft = "", forbidQuestions = false, combinedAnswerClarify = false) {
  const normalized = history
    .slice(-10)
    .map((item) => {
      const sender = item?.sender === "me" ? "Я" : "Кандидат";
      const text = (item?.text || "").trim().replace(/\s+/g, " ");
      return `${sender}: ${text}`;
    })
    .join("\n");
  const draftBlock = draft ? `\nЧернетка HR (можна переформулювати): ${draft}` : "";
  const forbidBlock = forbidQuestions ? "\nВАЖЛИВО: не став жодних запитань і не використовуй '?'.\n" : "";
  const combinedBlock = combinedAnswerClarify
    ? "\nВАЖЛИВО: сформуй ОДНЕ цілісне повідомлення: коротка відповідь по суті + ОДНЕ м'яке уточнююче питання в кінці. Не розбивай на два повідомлення.\n"
    : "";
  return `${HR_ASSISTANT_PROMPT}${forbidBlock}${combinedBlock}\nОстанні повідомлення (від старих до нових):\n${normalized || "(історія пуста)"}${draftBlock}\n\nСформуй ОДНУ коротку відповідь без нумерації і без пояснень (до 3 коротких речень).`;
}

function buildStopPrompt(history = [], lastMessage = "") {
  const normalized = history
    .slice(-10)
    .map((item) => {
      const sender = item?.sender === "me" ? "Я" : "Кандидат";
      const text = (item?.text || "").trim().replace(/\s+/g, " ");
      return `${sender}: ${text}`;
    })
    .join("\n");
  const lastLine = lastMessage ? `Останнє повідомлення кандидата: ${lastMessage}` : "";
  return `${STOP_CLASSIFY_PROMPT}\n\nІсторія:\n${normalized || "(порожньо)"}\n${lastLine}`;
}

function buildFormatChoicePrompt(history = [], lastMessage = "") {
  const normalized = history
    .slice(-10)
    .map((item) => {
      const sender = item?.sender === "me" ? "Я" : "Кандидат";
      const text = (item?.text || "").trim().replace(/\s+/g, " ");
      return `${sender}: ${text}`;
    })
    .join("\n");
  const lastLine = lastMessage ? `Останнє повідомлення кандидата: ${lastMessage}` : "";
  return `${FORMAT_CHOICE_PROMPT}\n\nІсторія:\n${normalized || "(порожньо)"}\n${lastLine}`;
}

function buildIntentPrompt(history = [], lastMessage = "") {
  const normalized = history
    .slice(-10)
    .map((item) => {
      const sender = item?.sender === "me" ? "Я" : "Кандидат";
      const text = (item?.text || "").trim().replace(/\s+/g, " ");
      return `${sender}: ${text}`;
    })
    .join("\n");
  const lastLine = lastMessage ? `Останнє повідомлення кандидата: ${lastMessage}` : "";
  return `${INTENT_CLASSIFY_PROMPT}\n\nІсторія:\n${normalized || "(порожньо)"}\n${lastLine}`;
}

app.post("/dialog_suggest", async (req, res) => {
  try {
    const { history = [], draft = "", no_questions = false, combined_answer_clarify = false } = req.body || {};
    const prompt = buildHistoryPrompt(history, draft, Boolean(no_questions), Boolean(combined_answer_clarify));
    const { text, raw } = await callOpenAI({ system: "Ти — HR Furioza. Відповідай коротко.", user: prompt });
    return res.json({ ok: true, text: (text || "").trim(), raw });
  } catch (error) {
    console.error("/dialog_suggest error", error);
    return res.status(500).json({ ok: false, error: error.message });
  }
});

app.post("/should_pause", async (req, res) => {
  try {
    const { history = [], last_message = "" } = req.body || {};
    const prompt = buildStopPrompt(history, last_message);
    const { text } = await callOpenAI({
      system: "You are a strict classifier.",
      user: prompt,
      temperature: 0,
    });
    const normalized = (text || "").trim().toLowerCase();
    const stop = normalized.startsWith("stop");
    return res.json({ ok: true, stop, text: (text || "").trim() });
  } catch (error) {
    console.error("/should_pause error", error);
    return res.status(500).json({ ok: false, error: error.message });
  }
});

app.post("/format_choice", async (req, res) => {
  try {
    const { history = [], last_message = "" } = req.body || {};
    const prompt = buildFormatChoicePrompt(history, last_message);
    const { text } = await callOpenAI({
      system: "You are a strict format classifier.",
      user: prompt,
      temperature: 0,
    });
    const normalized = (text || "").trim().toLowerCase();
    let choice = "unknown";
    if (normalized.startsWith("both")) {
      choice = "both";
    } else if (normalized.startsWith("mini_course") || normalized.startsWith("mini")) {
      choice = "mini_course";
    } else if (normalized.startsWith("video")) {
      choice = "video";
    }
    return res.json({ ok: true, choice, text: (text || "").trim() });
  } catch (error) {
    console.error("/format_choice error", error);
    return res.status(500).json({ ok: false, error: error.message });
  }
});

app.post("/intent_classify", async (req, res) => {
  try {
    const { history = [], last_message = "" } = req.body || {};
    const prompt = buildIntentPrompt(history, last_message);
    const { text } = await callOpenAI({
      system: "You are a strict intent classifier.",
      user: prompt,
      temperature: 0,
    });
    const normalized = (text || "").trim().toLowerCase();
    let intent = "other";
    if (normalized.startsWith("question")) {
      intent = "question";
    } else if (normalized.startsWith("ack_continue") || normalized.startsWith("ack")) {
      intent = "ack_continue";
    } else if (normalized.startsWith("stop")) {
      intent = "stop";
    }
    return res.json({ ok: true, intent, text: (text || "").trim() });
  } catch (error) {
    console.error("/intent_classify error", error);
    return res.status(500).json({ ok: false, error: error.message });
  }
});

app.use((err, req, res, next) => {
  console.error("Unhandled error:", err);
  if (res.headersSent) return next(err);
  res.status(500).json({ ok: false, error: err?.message || "Internal Server Error" });
});

app.listen(PORT, () => console.log(`AI server on :${PORT}`));
