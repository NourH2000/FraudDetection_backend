var { PythonShell } = require('python-shell');

const  pythonShellScript =  ( path , options ) => {
    
    PythonShell.run(path, options, function (err, results) {
        if (err) throw err;
        console.log('results: %j', results);
        console.log(' the model has been called');
      });
}

module.exports = { pythonShellScript };

/* */