'use strict';

describe('Service: logger', function () {

  // load the service's module
  beforeEach(module('impala2GoApp.services'));

  // instantiate service
  var logger;
  beforeEach(inject(function (_logger_) {
      logger = _logger_;
  }));

  it('should do something', function () {
    expect(!!logger).toBe(true);
  });

});
