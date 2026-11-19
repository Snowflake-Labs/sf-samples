// Runs the real tools against the demo pipeline in your account. Opt-in:
//   IR_CONTRACT=1 IR_CONNECTION=<connection name> npx vitest run __tests__/contract
// Leaves the pipeline reset (demo/reset.sql) when done.
import { execFileSync } from "node:child_process"
import { afterAll, beforeAll, describe, expect, it } from "vitest"

const CONN = process.env.IR_CONNECTION ?? ""
const run = !!process.env.IR_CONTRACT && !!CONN
const INC = "INC-CONTRACT-1"

function sql(q: string): Record<string, unknown>[] {
  const out = execFileSync("snow", ["sql", "--connection", CONN, "--format", "json", "-q", q], {
    encoding: "utf8",
    maxBuffer: 32 * 1024 * 1024,
  })
  const parsed = JSON.parse(out.slice(out.indexOf("[")))
  // Multi-statement output is an array of result arrays; return the last.
  return Array.isArray(parsed[0]) ? parsed.at(-1) : parsed
}
const file = (f: string) => execFileSync("snow", ["sql", "--connection", CONN, "-f", f], { encoding: "utf8" })
const call = (q: string) => {
  const v = Object.values(sql(q)[0])[0]
  return typeof v === "string" ? JSON.parse(v) : v
}

const FIX = `INSERT OVERWRITE INTO INCIDENT_ROOM_DEMO.SALES.DAILY_REVENUE
SELECT ORDER_DATE, REGION, SUM(ORDER_AMOUNT), COUNT(*) FROM INCIDENT_ROOM_DEMO.SALES.RAW_ORDERS GROUP BY ORDER_DATE, REGION`

describe.skipIf(!run)("Multi-player AI Incident Room tools (contract)", () => {
  beforeAll(() => {
    file("demo/reset.sql")
    sql(`INSERT INTO INCIDENT_ROOM.APP.INCIDENTS (ID, TITLE, STATUS, OPENED_BY) SELECT '${INC}', 'contract', 'TRIAGE', 'TEST'`)
    file("demo/break.sql")
  }, 300_000)
  afterAll(() => {
    file("demo/reset.sql")
  }, 300_000)

  it("Triage sees the failed run with the missing-column error", async () => {
    // Task history can lag a few seconds behind EXECUTE TASK.
    let failure: { state: string; error_message: string } | undefined
    for (let i = 0; i < 12 && !failure; i++) {
      const runs = call("CALL INCIDENT_ROOM.AGENTS.RECENT_FAILURES()") as { state: string; error_message: string }[]
      failure = runs.find((r) => r.state === "FAILED")
      if (!failure) await new Promise((r) => setTimeout(r, 5000))
    }
    expect(failure?.error_message).toMatch(/AMOUNT/i)
  }, 120_000)

  it("Triage can read the task SQL, the renamed column, and the blast radius", () => {
    const p = call("CALL INCIDENT_ROOM.AGENTS.INSPECT_PIPELINE()")
    expect(p.task_sql).toMatch(/SUM\(AMOUNT\)/)
    expect(p.columns.RAW_ORDERS.join(" ")).toMatch(/ORDER_AMOUNT/)
    const views = (call("CALL INCIDENT_ROOM.AGENTS.DOWNSTREAM('DAILY_REVENUE')") as { object: string; hops: number }[]).map((v) => v.object)
    expect(views).toEqual(expect.arrayContaining(["INCIDENT_ROOM_DEMO.SALES.REVENUE_BY_REGION_V", "INCIDENT_ROOM_DEMO.SALES.EXEC_SCORECARD_V"]))
  }, 120_000)

  it("the guard refuses a patch that reaches outside the schema, without running it", () => {
    const r = call(`CALL INCIDENT_ROOM.AGENTS.PROPOSE_AND_TEST('${INC}', 'INSERT INTO INCIDENT_ROOM.APP.VOTES SELECT 1,2,3,4')`)
    expect(r.ok).toBe(false)
    expect(r.error).toMatch(/outside/)
    expect(sql("SELECT COUNT(*) N FROM INCIDENT_ROOM.APP.VOTES")[0].N).toBe(0)
  }, 120_000)

  it("the correct fix passes on a zero-copy clone and prod is untouched", () => {
    const before = sql("SELECT COUNT(*) N FROM INCIDENT_ROOM_DEMO.SALES.DAILY_REVENUE")[0].N
    const r = call(`CALL INCIDENT_ROOM.AGENTS.PROPOSE_AND_TEST('${INC}', '${FIX.replace(/'/g, "''")}')`)
    expect(r).toMatchObject({ ok: true, clone_schema: "INCIDENT_ROOM_CLONES.INC_CONTRACT_1", verdict: expect.stringMatching(/matches/) })
    expect(sql("SELECT COUNT(*) N FROM INCIDENT_ROOM_DEMO.SALES.DAILY_REVENUE")[0].N).toBe(before)
    const task = call("CALL INCIDENT_ROOM.AGENTS.INSPECT_PIPELINE()").task_sql
    expect(task).toMatch(/SUM\(AMOUNT\)/) // prod task unchanged
  }, 180_000)

  it("SHIP_FIX refuses without two human approvals, and ignores agent votes", () => {
    const patch = sql(`SELECT ID FROM INCIDENT_ROOM.APP.PATCHES WHERE INCIDENT_ID = '${INC}' AND TESTED_OK`)[0].ID as string
    sql(`UPDATE INCIDENT_ROOM.APP.INCIDENTS SET STATUS = 'VOTING' WHERE ID = '${INC}'`)
    sql(`INSERT INTO INCIDENT_ROOM.APP.VOTES (PATCH_ID, VOTER, VOTE) VALUES ('${patch}', 'DASH', 'approve'), ('${patch}', 'FIXER', 'approve')`)
    expect(call(`CALL INCIDENT_ROOM.AGENTS.SHIP_FIX('${patch}', 'DASH')`)).toMatchObject({ ok: false, error: "1 of 2 approvals" })
    sql(`INSERT INTO INCIDENT_ROOM.APP.VOTES (PATCH_ID, VOTER, VOTE) VALUES ('${patch}', 'CLAIRE', 'approve')`)
    expect(call(`CALL INCIDENT_ROOM.AGENTS.SHIP_FIX('${patch}', 'EVA')`)).toMatchObject({ ok: false, error: /approved can ship/ })
    expect(call(`CALL INCIDENT_ROOM.AGENTS.SHIP_FIX('${patch}', 'CLAIRE')`)).toMatchObject({ ok: true })
    expect(call("CALL INCIDENT_ROOM.AGENTS.INSPECT_PIPELINE()").task_sql).toMatch(/ORDER_AMOUNT/)
  }, 180_000)
})
