import { describe, expect, it } from "vitest"
import { describeProperty } from "@/lib/describe-property"

describe("describeProperty", () => {
  it("reads the current DESCRIBE USER shape, including uppercase driver keys", () => {
    const rows = [
      { PROPERTY: "FIRST_NAME", PROPERTY_TYPE: "String", PROPERTY_VALUE: "Ada", PROPERTY_DEFAULT: null },
      { PROPERTY: "DISPLAY_NAME", PROPERTY_VALUE: "Ada Lovelace" },
    ]
    expect(describeProperty(rows, "FIRST_NAME")).toBe("Ada")
    expect(describeProperty(rows, "DISPLAY_NAME")).toBe("Ada Lovelace")
  })

  it("reads the older property/value shape", () => {
    const rows = [{ property: "FIRST_NAME", value: "Grace", default: null, description: "First name" }]
    expect(describeProperty(rows, "first_name")).toBe("Grace")
  })

  it("does not fall through to value when property_value is present but empty", () => {
    const rows = [{ property: "FIRST_NAME", property_value: null, value: "Should not win" }]
    expect(describeProperty(rows, "FIRST_NAME")).toBeNull()
  })

  it("returns undefined when the property is absent", () => {
    expect(describeProperty([{ PROPERTY: "EMAIL", PROPERTY_VALUE: "a@b.c" }], "FIRST_NAME")).toBeUndefined()
  })
})
