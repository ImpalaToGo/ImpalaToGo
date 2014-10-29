'use strict';

describe('Service: dataContext', function () {

  // load the service's module
  beforeEach(module('impala2GoApp.services'));

  // instantiate service
  var dataContext;
  beforeEach(inject(function (_dataContext_) {
    dataContext = _dataContext_;
  }));

  it('should do something', function () {
    expect(!!dataContext).toBe(true);
  });

});
