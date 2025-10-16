class ApiService {
  constructor() {
    // If in dev, add localhost:8080
    if (import.meta.env.DEV) {
      this.baseUrl = "http://localhost:8080/api";
    } else {
      this.baseUrl = "/api";
    }
  }

  async getModels() {
    try {
      const response = await fetch(`${this.baseUrl}/models/`);
      const data = await response.json();
      return data.Models;
    } catch (error) {
      console.error("Failed to fetch models:", error);
      throw error;
    }
  }

  async streamChat(
    model,
    messages,
    onMessage,
    onError,
    onStatusChange,
    onSQLExecResult,
    onComplete
  ) {
    let abortController = new AbortController();

    try {
      const requestConfig = {
        method: "POST",
        signal: abortController.signal,
        headers: {
          "Content-Type": "application/json",
          Accept: "text/event-stream",
        },
        body: JSON.stringify({
          name: model,
          messages,
        }),
      };

      // Use POST request with fetch for streaming
      const response = await fetch(`${this.baseUrl}/message/`, requestConfig);

      if (!response.ok) {
        throw new Error(
          `HTTP error: status: ${response.status} ${response.statusText}`
        );
      }

      // Get the reader from the response body
      const reader = response.body
        .pipeThrough(new TextDecoderStream())
        .getReader();

      let buffer = "";

      // Process the stream
      while (true) {
        const { value, done } = await reader.read();

        if (done) {
          break;
        }

        buffer += value;

        // Process complete events from buffer
        const events = this.parseSSEBuffer(buffer);

        for (const event of events.complete) {
          this.handleSSEEvent(
            event,
            onMessage,
            onStatusChange,
            onSQLExecResult,
            onComplete
          );
        }

        // Keep incomplete data in buffer
        buffer = events.remaining;
      }
    } catch (error) {
      if (error.name !== "AbortError") {
        onError(error);
      }
    }

    // Return cleanup function
    return () => abortController.abort();
  }

  parseSSEBuffer(buffer) {
    const complete = [];
    let remaining = "";

    // Split by double newlines to separate events
    const chunks = buffer.split("\n\n");

    // All but the last chunk are complete events
    for (let i = 0; i < chunks.length - 1; i++) {
      const eventData = this.parseSSEEvent(chunks[i]);
      if (eventData) {
        complete.push(eventData);
      }
    }

    // Last chunk might be incomplete
    remaining = chunks[chunks.length - 1];

    return { complete, remaining };
  }

  parseSSEEvent(eventText) {
    const lines = eventText.split("\n");
    const event = {
      type: "message",
      data: "",
      id: null,
      retry: null,
    };

    for (const line of lines) {
      if (line.trim() === "") continue;

      const colonIndex = line.indexOf(":");
      if (colonIndex === -1) continue;

      const field = line.substring(0, colonIndex).trim();
      const value = line.substring(colonIndex + 1).trim();

      switch (field) {
        case "event":
          event.type = value;
          break;
        case "data":
          event.data += (event.data ? "\n" : "") + value;
          break;
        case "id":
          event.id = value;
          break;
        case "retry":
          event.retry = parseInt(value, 10);
          break;
      }
    }

    return event.data ? event : null;
  }

  handleSSEEvent(
    event,
    onMessage,
    onStatusChange,
    onSQLExecResult,
    onComplete
  ) {
    if (!event.data) return;

    try {
      // Handle based on event type (like original EventSource)
      switch (event.type) {
        case "status":
          const status = JSON.parse(event.data);
          if (status.status === "done") {
            onComplete();
          } else {
            onStatusChange(status.status_message);
          }
          break;

        case "message.content.delta":
          const content = JSON.parse(event.data);
          onMessage(content);
          break;

        case "sql.metadata":
          const metadata = JSON.parse(event.data);
          onSQLExecResult({
            metadata: metadata.metadata,
          });
          break;

        case "sql.data":
          const sqlData = JSON.parse(event.data);
          onSQLExecResult({
            data: sqlData.data,
          });
          break;

        default:
          // Handle default message type
          const parsedData = JSON.parse(event.data);
          onMessage(parsedData);
          break;
      }
    } catch (error) {
      console.error("Error parsing event data:", error, event);
    }
  }
}

export default new ApiService();
