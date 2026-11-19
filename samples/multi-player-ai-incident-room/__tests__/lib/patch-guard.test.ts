import { describe, expect, it } from "vitest"
import { checkTaskSql, cloneSchemaName, rewriteForClone } from "@/lib/domain/patch-guard"

const GOOD = `INSERT OVERWRITE INTO INCIDENT_ROOM_DEMO.SALES.DAILY_REVENUE
SELECT ORDER_DATE, REGION, SUM(ORDER_AMOUNT), COUNT(*) FROM INCIDENT_ROOM_DEMO.SALES.RAW_ORDERS GROUP BY ORDER_DATE, REGION`

describe("checkTaskSql", () => {
  it("accepts one INSERT that only touches the incident's schema", () => {
    expect(checkTaskSql(GOOD, "INCIDENT_ROOM_DEMO.SALES")).toEqual({ ok: true })
  })
  it("rejects anything but a single INSERT", () => {
    expect(checkTaskSql("DROP TABLE INCIDENT_ROOM_DEMO.SALES.RAW_ORDERS", "INCIDENT_ROOM_DEMO.SALES").ok).toBe(false)
    expect(checkTaskSql(`${GOOD}; DROP TABLE INCIDENT_ROOM_DEMO.SALES.RAW_ORDERS`, "INCIDENT_ROOM_DEMO.SALES").ok).toBe(false)
  })
  it("allows a trailing semicolon", () => {
    expect(checkTaskSql(`${GOOD};`, "INCIDENT_ROOM_DEMO.SALES").ok).toBe(true)
  })
  it("rejects references outside the schema", () => {
    expect(checkTaskSql(GOOD.replace("INCIDENT_ROOM_DEMO.SALES.RAW_ORDERS", "OTHER_DB.X.RAW_ORDERS"), "INCIDENT_ROOM_DEMO.SALES")).toEqual({
      ok: false,
      reason: expect.stringMatching(/OTHER_DB\.X/),
    })
  })
  it("rejects longer dotted chains and quoted identifiers that could hide another schema", () => {
    expect(checkTaskSql(GOOD.replace("RAW_ORDERS", "OTHER_DB.X.RAW_ORDERS"), "INCIDENT_ROOM_DEMO.SALES").ok).toBe(false)
    expect(checkTaskSql(GOOD.replace("INCIDENT_ROOM_DEMO.SALES.RAW_ORDERS", '"OTHER"."X"."T"'), "INCIDENT_ROOM_DEMO.SALES").ok).toBe(false)
  })
})

describe("clone naming", () => {
  it("derives a safe schema name inside INCIDENT_ROOM_CLONES", () => {
    expect(cloneSchemaName("INC-20261105-ab12")).toBe("INCIDENT_ROOM_CLONES.INC_20261105_AB12")
    expect(() => cloneSchemaName("x; drop")).toThrow()
  })
  it("rewrites every reference to the clone schema", () => {
    const out = rewriteForClone(GOOD, "INCIDENT_ROOM_DEMO.SALES", "INCIDENT_ROOM_CLONES.INC_1")
    expect(out).not.toMatch(/INCIDENT_ROOM_DEMO/)
    expect(out.match(/INCIDENT_ROOM_CLONES\.INC_1\./g)).toHaveLength(2)
  })
})
