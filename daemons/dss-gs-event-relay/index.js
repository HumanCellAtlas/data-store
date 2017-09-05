const runtimeConfig = require('cloud-functions-runtime-config');
const AWS = require('aws-sdk');

// FIXME: aws creds should be grabbed for a role, not borrowed from Travis
function dss_gs_event_relay(event, callback) {
  Promise.all(
    [runtimeConfig.getVariable(process.env.ENTRY_POINT, 'AWS_ACCESS_KEY_ID'),
     runtimeConfig.getVariable(process.env.ENTRY_POINT, 'AWS_SECRET_ACCESS_KEY'),
     runtimeConfig.getVariable(process.env.ENTRY_POINT, 'AWS_REGION'),
     runtimeConfig.getVariable(process.env.ENTRY_POINT, 'sns_topic_arn')]
  ).then(values => {
    var [AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION, sns_topic_arn] = values;

    AWS.config.update({
      accessKeyId: AWS_ACCESS_KEY_ID,
      secretAccessKey: AWS_SECRET_ACCESS_KEY,
      region: AWS_REGION
    });

    var sns = new AWS.SNS();

    const file = event.data;
    if (file.resourceState === 'not_exists') {
      console.log(`File ${file.name} deleted.`);
      callback();
    } else if (file.metageneration === '1') {
      // metageneration attribute is updated on metadata changes.
      // on create value is 1
      console.log(`File ${file.name} uploaded.`);

      sns.publish({
        TargetArn: sns_topic_arn,
        Message: JSON.stringify(event)
      }, function(err, data) {
        if (err) {
          console.log(err.stack);
          return;
        }
        console.log(`Sent push notification to ${sns_topic_arn}`);
        console.log(data);
        callback();
      });
    } else {
      console.log(`File ${file.name} metadata updated.`);
      callback();
    }
  });
}

exports[process.env.ENTRY_POINT] = dss_gs_event_relay;
