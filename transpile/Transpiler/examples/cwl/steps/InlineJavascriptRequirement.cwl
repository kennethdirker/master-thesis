cwlVersion: v1.2
class: CommandLineTool
baseCommand: hostname
id: InlineJavascriptRequirement
inputs: []
outputs:
    hostname:
        type: stdout
stdout: hostname.txt
requirements:
    - class: InlineJavascriptRequirement
      expressionLib:
        # - { $include: utils.js }
        - var foo = 1
        - var bar = 2
        - |
          /**
          * A merely illustrative example function that uses a function
          * from the included custom-functions.js file to create a
          * Hello World message.
          *
          * @param {Object} message - CWL document input message
          */
          var createHelloWorldMessage = function (message) {
            return capitalizeWords(message);
          };