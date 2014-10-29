'use strict';

describe('Controller: QueryCtrl', function () {

  // load the controller's module
  beforeEach(module('impala2GoApp'));
    beforeEach(module('impala2GoApp.services'));

  var QueryCtrl,
      scope,loggerService;

  // Initialize the controller and a mock scope
  beforeEach(inject(function ($controller, $rootScope,_logger_) {
      scope = $rootScope.$new();
      loggerService=_logger_;
    QueryCtrl = $controller('QueryCtrl', {
        $scope: scope,
        logger:loggerService
    });
  }));

    it('should check if logger is defined', function () {
        expect(loggerService).toBeDefined();
  });
});
