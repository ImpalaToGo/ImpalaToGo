'use strict';

describe('Controller: Resizetrl', function () {

  // load the controller's module
  beforeEach(module('impala2GoApp'));
    beforeEach(module('impala2GoApp.services'));

  var RedimCtrl,
      scope,loggerService;

  // Initialize the controller and a mock scope
  beforeEach(inject(function ($controller, $rootScope,_logger_) {
      scope = $rootScope.$new();
      loggerService=_logger_;
    RedimCtrl = $controller('Resizetrl', {
        $scope: scope,
        logger:loggerService
    });
  }));

    it('should check if logger is defined', function () {
        expect(loggerService).toBeDefined();
  });
});
