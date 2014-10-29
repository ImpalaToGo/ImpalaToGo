'use strict';

describe('Service: utilities', function () {

  // load the service's module
  beforeEach(module('impala2GoApp.services'));

  // instantiate service
  var utilities;
  beforeEach(inject(function (_utilities_) {
    utilities = _utilities_;
  }));

  it('should do something', function () {
    expect(!!utilities).toBe(true);
  });

});
