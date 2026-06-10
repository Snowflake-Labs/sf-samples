"use client";

import { CopilotChat } from "@copilotkit/react-ui";

const suggestions = [
  {
    title: "Search",
    message: "What objections came up in recent calls about our Alpine Pro boots?",
  },
  {
    title: "Analyze",
    message: "What is our win rate by territory this quarter?",
  },
  {
    title: "Combine",
    message: "How is Sophie Nakamura performing in the Northeast, and what themes come up in her sales calls?",
  },
];

export default function Home() {
  return (
    <main className="relative min-h-screen overflow-hidden bg-[radial-gradient(circle_at_top,_rgba(56,189,248,0.18),_transparent_35%),linear-gradient(180deg,_#07111f_0%,_#030712_100%)] text-white">
      <div className="pointer-events-none absolute inset-0 bg-[linear-gradient(to_right,rgba(148,163,184,0.08)_1px,transparent_1px),linear-gradient(to_bottom,rgba(148,163,184,0.08)_1px,transparent_1px)] bg-[size:72px_72px] opacity-25" />
      <section className="relative mx-auto flex min-h-screen w-full max-w-7xl flex-col gap-10 px-6 py-8 md:px-10 lg:px-12">
        <header className="flex items-center justify-between gap-4 rounded-full border border-white/10 bg-white/5 px-5 py-3 backdrop-blur-xl">
          <div>
            <p className="text-xs uppercase tracking-[0.35em] text-cyan-300/80">
              CopilotKit App
            </p>
            <h1 className="mt-1 text-lg font-semibold text-white">
              Snowflake Copilot Chat
            </h1>
          </div>
          <div className="hidden rounded-full border border-cyan-400/30 bg-cyan-400/10 px-3 py-1 text-xs font-medium text-cyan-100 md:block">
            Connected to /api/copilotkit
          </div>
        </header>

        <div className="grid flex-1 gap-6 lg:grid-cols-[0.95fr_1.05fr]">
          <aside className="flex flex-col justify-between rounded-[2rem] border border-white/10 bg-white/5 p-6 shadow-[0_30px_90px_rgba(2,6,23,0.45)] backdrop-blur-xl md:p-8">
            <div className="space-y-6">
              <div className="max-w-xl space-y-4">
                <p className="text-sm font-medium uppercase tracking-[0.28em] text-sky-300/80">
                  Ask the assistant
                </p>
                <h2 className="text-4xl font-semibold tracking-tight text-white md:text-5xl">
                  A focused chat surface for Snowflake-backed copilots.
                </h2>
                <p className="max-w-lg text-base leading-7 text-slate-300 md:text-lg">
                  Use the chat to prototype workflows, test runtime behavior, and
                  grow the agent from a clean starting point.
                </p>
              </div>

              <div className="grid gap-4 sm:grid-cols-2">
                <div className="rounded-2xl border border-white/10 bg-slate-950/50 p-4">
                  <p className="text-xs uppercase tracking-[0.24em] text-slate-400">
                    Runtime
                  </p>
                  <p className="mt-2 text-sm text-slate-200">
                    CopilotKit Provider at <span className="text-cyan-300">/api/copilotkit</span>
                  </p>
                </div>
                <div className="rounded-2xl border border-white/10 bg-slate-950/50 p-4">
                  <p className="text-xs uppercase tracking-[0.24em] text-slate-400">
                    Interface
                  </p>
                  <p className="mt-2 text-sm text-slate-200">
                    Streamlined chat with starter prompts and full conversation history.
                  </p>
                </div>
              </div>
            </div>

            <div className="mt-8 rounded-[1.5rem] border border-cyan-400/20 bg-cyan-400/10 p-5 text-sm leading-7 text-cyan-50">
              Try asking for an architecture walkthrough, a feature idea, or a
              concrete implementation plan. The chat is wired for immediate agent
              interaction.
            </div>
          </aside>

          <section className="h-[72vh] overflow-hidden rounded-[2rem] border border-white/10 bg-[#f8fafc] text-slate-900 shadow-[0_30px_90px_rgba(2,6,23,0.35)]">
            <CopilotChat
              className="h-full min-h-0 w-full overflow-auto rounded-[2rem]"
              suggestions={suggestions}
              disableSystemMessage
              labels={{
                title: "Copilot Chat",
                initial:
                  "Start here. Ask the assistant to explain the app, suggest a feature, or help you build the next piece.",
              }}
            />
          </section>
        </div>
      </section>
    </main>
  );
}
