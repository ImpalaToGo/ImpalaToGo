'use strict';

describe('Controller: SettingsCtrl', function () {

  // load the controller's module
  beforeEach(module('impala2GoApp'));
  beforeEach(module('impala2GoApp.services'));

  var SettingsCtrl,
      scope,loggerService;

  // Initialize the controller and a mock scope
  beforeEach(inject(function ($controller, $rootScope,_logger_) {
      scope = $rootScope.$new();
      loggerService=_logger_;
    SettingsCtrl = $controller('SettingsCtrl', {
        $scope: scope,
        logger:loggerService
    });
  }));

    it('should check if logger is defined', function () {
        expect(loggerService).toBeDefined();
  });
});
