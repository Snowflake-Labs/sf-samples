import { NextRequest, NextResponse } from "next/server";

type AgUiContentPart = {
  type?: string;
  text?: string;
};

type AgUiMessage = {
  role?: string;
  content?: string | AgUiContentPart[];
};

type SnowflakeContentBlock = {
  type?: string;
  id?: string;
  name?: string;
  input?: unknown;
  text?: string;
  thinking?: unknown;
  content?: unknown;
  tool_use_id?: string;
  toolUseId?: string;
  tool_use?: unknown;
  tool_result?: unknown;
  suggested_queries?: unknown;
};

const isRecord = (value: unknown): value is Record<string, unknown> => {
  return typeof value === "object" && value !== null && !Array.isArray(value);
};

const stringifyForChat = (value: unknown) => {
  if (typeof value === "string") {
    return value;
  }

  return JSON.stringify(value ?? "", null, 2);
};

const parseJsonIfPossible = (value: unknown): unknown => {
  if (typeof value !== "string") {
    return value;
  }

  const fencedJson = value.match(/```json\s*([\s\S]*?)\s*```/i);
  const candidate = fencedJson?.[1] ?? value;

  try {
    return JSON.parse(candidate);
  } catch {
    return value;
  }
};

const getTextFromContent = (content: AgUiMessage["content"]) => {
  if (typeof content === "string") {
    return content;
  }

  if (Array.isArray(content)) {
    return content
      .filter((part) => part.type === "text" && typeof part.text === "string")
      .map((part) => part.text)
      .join("");
  }

  return "";
};

const getUserPrompt = (body: {
  message?: unknown;
  prompt?: unknown;
  text?: unknown;
  messages?: unknown;
}) => {
  if (typeof body.message === "string") {
    return body.message.trim();
  }

  if (typeof body.prompt === "string") {
    return body.prompt.trim();
  }

  if (typeof body.text === "string") {
    return body.text.trim();
  }

  if (!Array.isArray(body.messages)) {
    return "";
  }

  const lastUserMessage = [...body.messages]
    .reverse()
    .find((message): message is AgUiMessage => {
      return (
        typeof message === "object" &&
        message !== null &&
        (message as AgUiMessage).role === "user"
      );
    });

  return getTextFromContent(lastUserMessage?.content).trim();
};

const jsonSse = (event: Record<string, unknown>) => {
  return `data: ${JSON.stringify(event)}\n\n`;
};

const sleep = (ms: number) => {
  return new Promise((resolve) => {
    setTimeout(resolve, ms);
  });
};

const getValue = (value: unknown, key: string) => {
  return isRecord(value) ? value[key] : undefined;
};

const getArrayValue = (value: unknown, key: string) => {
  const candidate = getValue(value, key);
  return Array.isArray(candidate) ? candidate : undefined;
};

const extractTextParts = (value: unknown): string[] => {
  const parsed = parseJsonIfPossible(value);

  if (parsed !== value) {
    return extractTextParts(parsed);
  }

  if (typeof value === "string") {
    return [value];
  }

  if (Array.isArray(value)) {
    return value.flatMap(extractTextParts);
  }

  if (!isRecord(value)) {
    return [];
  }

  if (value.type === "text" && typeof value.text === "string") {
    return [value.text];
  }

  return extractTextParts(value.content);
};

const getSnowflakeContentBlocks = (data: unknown): SnowflakeContentBlock[] => {
  const parsed = parseJsonIfPossible(data);
  const candidates = [
    getArrayValue(parsed, "content"),
    getArrayValue(getValue(parsed, "message"), "content"),
    getArrayValue(getValue(parsed, "response"), "content"),
    getArrayValue(getValue(parsed, "data"), "content"),
  ];

  const messages = getArrayValue(parsed, "messages") ?? getArrayValue(getValue(parsed, "data"), "messages");
  if (messages) {
    messages.forEach((message) => {
      candidates.push(getArrayValue(message, "content"));
    });
  }

  const response = parseJsonIfPossible(getValue(parsed, "response"));
  candidates.push(
    Array.isArray(response) ? response : undefined,
    getArrayValue(response, "content"),
    getArrayValue(getValue(response, "message"), "content")
  );

  return candidates
    .filter((candidate): candidate is unknown[] => Array.isArray(candidate))
    .flat()
    .filter((block): block is SnowflakeContentBlock => isRecord(block));
};

const getSnowflakeRecords = (data: unknown): Record<string, unknown>[] => {
  const blocks = getSnowflakeContentBlocks(data);
  const toolResultRecords = blocks.flatMap((block) => {
    const toolResult = isRecord(block.tool_result) ? block.tool_result : block;
    const content = Array.isArray(toolResult.content) ? toolResult.content : [];

    return content.flatMap((item) => {
      const json = getValue(item, "json");
      const resultSet = getValue(json, "result_set");
      const resultData = getValue(resultSet, "data");
      const metadata = getValue(resultSet, "resultSetMetaData");
      const rowType = getValue(metadata, "rowType");

      if (!Array.isArray(resultData) || !Array.isArray(rowType)) {
        return [];
      }

      const columns = rowType
        .map((column) => getValue(column, "name"))
        .filter((name): name is string => typeof name === "string");

      if (columns.length === 0) {
        return [];
      }

      return resultData
        .filter((row): row is unknown[] => Array.isArray(row))
        .map((row) => {
          return columns.reduce<Record<string, unknown>>((record, column, index) => {
            record[column] = row[index];
            return record;
          }, {});
        });
    });
  });

  const candidates = [
    getValue(data, "results"),
    getValue(data, "records"),
    getValue(data, "rows"),
    getValue(getValue(data, "data"), "results"),
    getValue(getValue(data, "data"), "rows"),
    toolResultRecords,
  ];

  const records = candidates.find((candidate): candidate is Record<string, unknown>[] => {
    return Array.isArray(candidate) && candidate.every(isRecord);
  });

  return records ?? [];
};

const recordsToMarkdownTable = (records: Record<string, unknown>[]) => {
  if (records.length === 0) {
    return "";
  }

  const columns = Array.from(
    records.reduce((keys, record) => {
      Object.keys(record).forEach((key) => keys.add(key));
      return keys;
    }, new Set<string>())
  );

  const formatCell = (value: unknown) => {
    const text = isRecord(value) || Array.isArray(value) ? JSON.stringify(value) : String(value ?? "");
    return text.replace(/\|/g, "\\|").replace(/\r?\n/g, " ");
  };

  return [
    `| ${columns.join(" | ")} |`,
    `| ${columns.map(() => "---").join(" | ")} |`,
    ...records.map((record) => `| ${columns.map((column) => formatCell(record[column])).join(" | ")} |`),
  ].join("\n");
};

const getSnowflakeChatOutput = (data: unknown, records: Record<string, unknown>[]) => {
  const blocks = getSnowflakeContentBlocks(data);
  const blockText = blocks
    .flatMap((block) => {
      if (block.type === "text" && typeof block.text === "string") {
        return [block.text];
      }

      if (block.type === "suggested_queries" && Array.isArray(block.suggested_queries)) {
        const suggestions = block.suggested_queries
          .map((suggestion) => getValue(suggestion, "query"))
          .filter((query): query is string => typeof query === "string");

        return suggestions.length > 0
          ? [`Suggested follow-ups:\n${suggestions.map((query) => `- ${query}`).join("\n")}`]
          : [];
      }

      return [];
    })
    .map((part) => part.trim())
    .filter(Boolean)
    .join("\n\n");

  if (blockText) {
    return blockText;
  }

  const directText = [
    getValue(data, "response"),
    getValue(data, "result"),
    getValue(data, "text"),
    getValue(data, "message"),
    getValue(data, "messages"),
    getValue(getValue(data, "data"), "response"),
    getValue(getValue(data, "data"), "messages"),
  ]
    .flatMap(extractTextParts)
    .map((part) => part.trim())
    .filter(Boolean)
    .join("\n\n");

  if (directText) {
    return directText;
  }

  const table = recordsToMarkdownTable(records);
  if (table) {
    return table;
  }

  return `Snowflake returned:\n\n\`\`\`json\n${JSON.stringify(data, null, 2)}\n\`\`\``;
};

const pushTextEvents = (
  events: Record<string, unknown>[],
  messageId: string,
  text: string
) => {
  if (!text.trim()) {
    return;
  }

  events.push(
    { type: "TEXT_MESSAGE_START", messageId, role: "assistant" },
    { type: "TEXT_MESSAGE_CONTENT", messageId, delta: text },
    { type: "TEXT_MESSAGE_END", messageId }
  );
};

const pushReasoningEvents = (
  events: Record<string, unknown>[],
  messageId: string,
  text: string
) => {
  const trimmedText = text.trim();

  if (!trimmedText) {
    return;
  }

  events.push(
    { type: "REASONING_START", messageId },
    { type: "REASONING_MESSAGE_START", messageId, role: "reasoning" },
    { type: "REASONING_MESSAGE_CONTENT", messageId, delta: trimmedText },
    { type: "REASONING_MESSAGE_END", messageId },
    { type: "REASONING_END", messageId }
  );

  pushTextEvents(events, crypto.randomUUID(), `**Thinking**\n\n${trimmedText}`);
};

const pushSnowflakeBlockEvents = (
  events: Record<string, unknown>[],
  blocks: SnowflakeContentBlock[],
  records: Record<string, unknown>[],
  fallbackText: string
) => {
  const pendingText: string[] = [];
  const seenToolIds = new Set<string>();
  let parentMessageId: string | undefined;

  const flushText = () => {
    const text = pendingText.join("\n\n").trim();
    pendingText.length = 0;

    if (!text) {
      return;
    }

    parentMessageId = crypto.randomUUID();
    pushTextEvents(events, parentMessageId, text);
  };

  blocks.forEach((block) => {
    switch (block.type) {
      case "thinking": {
        flushText();
        const thinking = isRecord(block.thinking) ? block.thinking : undefined;
        pushReasoningEvents(
          events,
          crypto.randomUUID(),
          typeof block.thinking === "string"
            ? block.thinking
            : typeof thinking?.text === "string"
              ? thinking.text
              : stringifyForChat(block.content ?? block.thinking)
        );
        break;
      }
      case "tool_use": {
        flushText();
        const toolUse = isRecord(block.tool_use) ? block.tool_use : block;
        const toolCallId = (typeof toolUse.tool_use_id === "string" && toolUse.tool_use_id) || block.id;
        const toolCallName =
          (typeof toolUse.name === "string" && toolUse.name) ||
          block.name ||
          "snowflake_tool";

        if (toolCallId) {
          seenToolIds.add(toolCallId);
          events.push(
            {
              type: "TOOL_CALL_START",
              toolCallId,
              toolCallName,
              parentMessageId,
            },
            {
              type: "TOOL_CALL_ARGS",
              toolCallId,
              delta: stringifyForChat(getValue(toolUse, "input") ?? block.input ?? {}),
            },
            { type: "TOOL_CALL_END", toolCallId }
          );
        }
        break;
      }
      case "tool_result": {
        flushText();
        const toolResult = isRecord(block.tool_result) ? block.tool_result : block;
        const toolCallId =
          (typeof toolResult.tool_use_id === "string" && toolResult.tool_use_id) ||
          block.tool_use_id ||
          block.toolUseId ||
          crypto.randomUUID();
        seenToolIds.add(toolCallId);
        events.push({
          type: "TOOL_CALL_RESULT",
          messageId: crypto.randomUUID(),
          toolCallId,
          content: stringifyForChat(getValue(toolResult, "content") ?? block.content ?? block.text ?? ""),
          role: "tool",
        });
        break;
      }
      case "text": {
        pendingText.push(typeof block.text === "string" ? block.text : stringifyForChat(block.content));
        break;
      }
      case "suggested_queries": {
        if (Array.isArray(block.suggested_queries)) {
          const suggestions = block.suggested_queries
            .map((suggestion) => getValue(suggestion, "query"))
            .filter((query): query is string => typeof query === "string");

          if (suggestions.length > 0) {
            pendingText.push(`Suggested follow-ups:\n${suggestions.map((query) => `- ${query}`).join("\n")}`);
          }
        }
        break;
      }
      default: {
        const text = extractTextParts(block).join("\n\n").trim();
        if (text) {
          pendingText.push(text);
        }
      }
    }
  });

  flushText();

  if (blocks.length === 0) {
    pushTextEvents(events, crypto.randomUUID(), fallbackText);
  }

  if (records.length > 0) {
    events.push({
      type: "STATE_SNAPSHOT",
      snapshot: { dashboardRecords: records },
    });
  }

  return seenToolIds;
};

export const POST = async (req: NextRequest) => {
  try {
    const body = await req.json();
    const userPrompt = getUserPrompt(body);

    if (!userPrompt) {
      return NextResponse.json(
        { error: "Missing user text in request body" },
        { status: 400 }
      );
    }

    const accountId = process.env.SNOWFLAKE_ACCOUNT;
    const database = process.env.SNOWFLAKE_DATABASE;
    const schema = process.env.SNOWFLAKE_SCHEMA;
    const agentName = process.env.SNOWFLAKE_AGENT_NAME;
    const token = process.env.SNOWFLAKE_TOKEN;

    const missingConfig = Object.entries({
      SNOWFLAKE_ACCOUNT: accountId,
      SNOWFLAKE_DATABASE: database,
      SNOWFLAKE_SCHEMA: schema,
      SNOWFLAKE_AGENT_NAME: agentName,
      SNOWFLAKE_TOKEN: token,
    })
      .filter(([, value]) => !value)
      .map(([key]) => key);

    if (missingConfig.length > 0) {
      return NextResponse.json(
        { error: `Missing Snowflake configuration: ${missingConfig.join(", ")}` },
        { status: 500 }
      );
    }

    const snowflakeUrl = new URL(
      `/api/v2/databases/${encodeURIComponent(database!)}/schemas/${encodeURIComponent(schema!)}/agents/${encodeURIComponent(agentName!)}:run`,
      `https://${accountId}.snowflakecomputing.com`
    );

    const snowflakePayload = {
      messages: [
        {
          role: "user",
          content: [{ type: "text", text: userPrompt }],
        },
      ],
      stream: false,
    };

    const snowflakeResponse = await fetch(snowflakeUrl, {
      method: "POST",
      headers: {
        Authorization: `Bearer ${token}`,
        "Content-Type": "application/json",
        Accept: "application/json",
      },
      body: JSON.stringify(snowflakePayload),
    });

    if (!snowflakeResponse.ok) {
      const errorText = await snowflakeResponse.text();
      return NextResponse.json(
        { error: `Snowflake Error: ${errorText}` },
        { status: snowflakeResponse.status }
      );
    }

    const data = await snowflakeResponse.json();
    const records = getSnowflakeRecords(data);
    const chatOutput = getSnowflakeChatOutput(data, records);
    const contentBlocks = getSnowflakeContentBlocks(data);
    const encoder = new TextEncoder();
    const threadId = typeof body.threadId === "string" ? body.threadId : crypto.randomUUID();
    const runId = typeof body.runId === "string" ? body.runId : crypto.randomUUID();

    const stream = new ReadableStream({
      start(controller) {
        void (async () => {
          const events: Record<string, unknown>[] = [
            { type: "RUN_STARTED", threadId, runId },
          ];

          pushSnowflakeBlockEvents(events, contentBlocks, records, chatOutput);
          events.push({ type: "RUN_FINISHED", threadId, runId });

          for (const event of events) {
            controller.enqueue(encoder.encode(jsonSse(event)));
            await sleep(180);
          }
        })()
          .catch(() => {
            controller.error(new Error("Failed to stream Snowflake agent events"));
          })
          .finally(() => {
            controller.close();
          });
      },
    });

    return new Response(stream, {
      headers: {
        "Content-Type": "text/event-stream",
        "Cache-Control": "no-cache",
        Connection: "keep-alive",
      },
    });
  } catch (error: unknown) {
    const message = error instanceof Error ? error.message : "Unknown server error";
    return NextResponse.json({ error: message }, { status: 500 });
  }
};
