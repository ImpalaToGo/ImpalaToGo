'use strict';

describe('Directive: someDirective', function () {

  // load the directive's module
  beforeEach(module('impala2GoApp.directives'));

  var element,
    scope;

  beforeEach(inject(function ($rootScope) {
    scope = $rootScope.$new();
  }));

  it('should make something  with element', inject(function ($compile) {

  }));
});
