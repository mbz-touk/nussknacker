const seed = "suggester"

describe("Expression suggester", () => {
  before(() => {
    cy.deleteAllTestProcesses({filter: seed, force: true})
  })

  after(() => {
    cy.deleteAllTestProcesses({filter: seed})
  })

  it("should display colorfull and sorted completions", () => {
    cy.visitNewProcess(seed, "variables")
    cy.get("[model-id=kafka-string]").trigger("dblclick")
    cy.get("[data-testid=node-modal]").as("modal")
    cy.get("[title=Expression]").next().find(".ace_editor").as("input")
    cy.get("@input").click().type(".")
    cy.get("@modal").toMatchExactImageSnapshot()
    cy.get("@input").type("c")
    cy.get("@modal").toMatchExactImageSnapshot()
  })
})
