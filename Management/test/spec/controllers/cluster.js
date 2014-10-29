'use strict';

describe('Controller: ClusterCtrl', function () {

    // load the controller's module
    beforeEach(module('impala2GoApp'));
    beforeEach(module('impala2GoApp.services'));

    var ClusterCtrl,
        scope,loggerService;

    // Initialize the controller and a mock scope
    beforeEach(inject(function ($controller, $rootScope,_logger_) {
        scope = $rootScope.$new();
        loggerService=_logger_;
        ClusterCtrl = $controller('ClusterCtrl', {
            $scope: scope,
            logger:loggerService
        });
    }));

    it('should check if logger is defined', function () {
        expect(loggerService).toBeDefined();
    });
    it('should check if gridOptions object is defined', function () {
        expect(scope.gridOptions).toBeDefined();
    });
//    it('should check if gridOptions object is defined', function () {
//        expect(scope.gridOptions.data).toBe();
//    });

});
