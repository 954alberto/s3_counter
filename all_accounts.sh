#!/usr/bin/env bash
export AWS_DEFAULT_SSO_REGION=
export AWS_DEFAULT_SSO_START_URL=

CMD="~/.local/bin/aws-sso-util"

#aws-sso-util roles --no-header
ROLES=$(aws-sso-util roles --no-header | awk {'print $1'})

for R in $ROLES
do
export AWS_PROFILE=$R
echo -e "\n\nAWS_ACCOUNT: $R\nBUCKETS:\n"
# Replace <profile-name> with your AWS CLI profile name
aws-sso-util login $R
aws --profile $R s3 ls | awk {'print $3'}
./s3_counter
done

