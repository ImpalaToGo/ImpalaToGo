'use strict';

var app=angular.module('impala2GoApp.config');
var remoteServiceName = 'breeze/Breeze';

// Configure Toastr
toastr.options.timeOut = 4000;
toastr.options.positionClass = 'toast-bottom-right';


app.config(['$logProvider', function ($logProvider) {
    // turn debugging off/on (no info or warn)
    if ($logProvider.debugEnabled) {
        $logProvider.debugEnabled(true);
    }
}]);

var config = {
    appErrorPrefix: '[impala2GoApp Error] ', //Configure the exceptionHandler decorator
    docTitle: 'impala2Go: ',
   // remoteServiceName: remoteServiceName,
    version: '1.0'
};

app.value('config', config);
