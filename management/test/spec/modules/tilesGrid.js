'use strict';

describe('Directive: TileGrid', function () {

  // load the directive's module
  beforeEach(module('impala2GoApp.tilesGrid'));

  var element,
    scope;

  beforeEach(inject(function ($rootScope) {
    scope = $rootScope.$new();
  }));

  it('should make something  with element', inject(function ($compile) {
    element = angular.element('<tile-grid></tile-grid>');
    element = $compile(element)(scope);
  //  expect(element.text()).toBe('this is the TileGrid directive');
  }));
});
