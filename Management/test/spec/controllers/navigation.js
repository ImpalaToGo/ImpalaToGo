'use strict';

describe('Controller: NavigationCtrl', function () {

    // load the controller's module
    beforeEach(module('impala2GoApp'));
    beforeEach(module('impala2GoApp.services'));

    var NavigationCtrl,
        scope,loggerService;

    // Initialize the controller and a mock scope
    beforeEach(inject(function ($controller, $rootScope,_logger_) {
        scope = $rootScope.$new();
        loggerService=_logger_;
        NavigationCtrl = $controller('NavigationCtrl', {
            $scope: scope,
            logger:loggerService
        });
    }));

    it('should check if logger is defined', function () {
        expect(loggerService).toBeDefined();
    });
    it('should check if isCurrent function is defined', function () {
        expect(scope.isCurrent).toBeDefined();
    });
});
