import * as chai from "chai";
import {Template} from "./../Template";

let expect = chai.expect;

describe("Example test", () => {
	it("should test something", (done) => {
		expect(Template.Hello()).to.deep.equal("Hello");
		done();
	});
});