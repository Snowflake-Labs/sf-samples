"use client"

import ReactMarkdown from "react-markdown"
import remarkGfm from "remark-gfm"

// Agent replies are markdown (tables, SQL blocks). Raw HTML is not rendered.
export function Markdown({ children }: { children: string }) {
  return (
    <div className="prose-room min-w-0 space-y-2 text-sm leading-relaxed [overflow-wrap:anywhere] [&_code]:rounded [&_code]:bg-muted [&_code]:px-1 [&_code]:py-0.5 [&_code]:font-mono [&_code]:text-[0.8em] [&_pre]:overflow-x-auto [&_pre]:rounded-md [&_pre]:bg-muted [&_pre]:p-3 [&_pre_code]:bg-transparent [&_pre_code]:p-0 [&_table]:w-full [&_table]:text-xs [&_td]:border-t [&_td]:px-2 [&_td]:py-1 [&_th]:px-2 [&_th]:py-1 [&_th]:text-left [&_ul]:list-disc [&_ul]:pl-5 [&_ol]:list-decimal [&_ol]:pl-5 [&_strong]:font-semibold">
      <ReactMarkdown remarkPlugins={[remarkGfm]} skipHtml>
        {children}
      </ReactMarkdown>
    </div>
  )
}
