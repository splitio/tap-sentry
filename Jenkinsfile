#!/usr/bin/env groovy

pipeline {
    agent any

    triggers {
      cron('H * * * *') // run every hour
    }

    options {
      buildDiscarder(logRotator(daysToKeepStr: '2'))
    }

    environment {
      SF_ROLE="SYSADMIN"
      SF_DATABASE="SPLIT"
      SF_WAREHOUSE="COMPUTE_WH"
      SF_CRED=credentials("SNOWFLAKE")
      SF_ACCOUNT="bv23770.us-east-1"

      // State File
      SENTRY_STATE="./states/sentry.json"

      // Python Enviroments
      VENV_S3="venv/tap-sentry"
      VENV_SF="venv/target-snowflake"

    }

    stages {

        stage('Create States directory') {
          steps {
            sh "mkdir -p ./states"
          }
        } // Stage States Directory

        stage('Create Venvs') {
          parallel {
            stage('Venv Sentry') {
              environment {
                SOURCE_INSTALL='.[dev]'
                FLAG="-e"
              }
              steps {
                sh './createVenv.sh "${VENV_SENTRY}" "${SOURCE_INSTALL}" "${FLAG}"'
              }
            }// stage Venv S3
            stage('Venv Snowflake') {
              environment {
                SOURCE_INSTALL='git+https://gitlab.com/meltano/target-snowflake.git@master#egg=target-snowflake'
                FLAG="-e"
              }
              steps {
                sh './createVenv.sh "${VENV_SF}" "${SOURCE_INSTALL}" "${FLAG}"'
              }
            } // Stage Venv Snowflake
            stage('State Sentry'){
              steps{
                setState("${SENTRY_STATE}")
              }
            }// stage State S3
          } // Parallel
        } // Stage Create Venv

        stage('Run Tap-sentry'){
          environment{
            SENTRY_START_DATE="2019-06-18"
            SENTRY_TOKEN=credentials('SENTRY_TOKEN')
            SF_SCHEMA="SENTRY"
            SF_CONFIG_FILE="config-snoflake-sentry.json"
            TAP_OUTPUT="tap-sentry-output.json"
            STDERRFILE="stderr_sentry.out"
          }
          steps{
            script{
              sh(returnStdout: false, script: 'set -euo pipefail')
              sh(returnStdout: false, script: 'envsubst < config-sentry.json.tpl > config-sentry.json')
              sh(returnStdout: false, script: 'envsubst < config-snowflake.json.tpl > "${SF_CONFIG_FILE}"')
              status=sh(returnStatus: true, script: '${VENV_SENTRY}/bin/tap-sentry -c config-sentry.json --catalog sentry-properties.json -s "${SENTRY_STATE}" > "${TAP_OUTPUT}" 2>"${STDERRFILE}"')
              catchError(status, "Tap-sentry", "Failed to collect data.", "${STDERRFILE}")
              status=sh(returnStdout: false, script:'echo -e "\n" >>  ${SENTRY_STATE}')
              status=sh(returnStatus: true, script: 'cat ${TAP_OUTPUT} | ${VENV_SF}/bin/target-snowflake -c "${SF_CONFIG_FILE}" >> ${SENTRY_STATE} 2>"${STDERRFILE}"')
              catchError(status, "Tap-sentry", "Failed to send data.", "${STDERRFILE}")
            }
          }
        }// stage Run Tap-sentry

    } // Stages

    post{

      success{
        slackSend(channel: "#analytics-alerts", message: "Tap-sentry Worked.", color: "#008000")
      }
      always{
        cleanWs (
          deleteDirs: false,
          patterns: [
            [pattern: 'config*.json', type: 'INCLUDE'],
            [pattern: '*output*.json', type: 'INCLUDE'],
            [pattern: 'stderr*.out', type: 'INCLUDE']
          ]
        )
      }//always
    }// post
} // Pipeline

def getCurrentMonth(){
  return sh(returnStdout:true, script:'date +%Y-%m').trim()
}

def setState(state){
  def exists = fileExists state
  if (exists) {
    def file = readFile state
    def last = file.split("\n")[file.split("\n").length-1]
    writeFile file: state, text : last
    def count = sh(returnStdout:true, script:'cat '+ state + ' | tr \' \' \'\n\' | grep bookmark | wc -l').trim()
    echo count
    sh(returnStdout:true, script:'cat ' + state)
  }
  else {
    writeFile file: state, text: '{}'
  }
}

def catchError(status, tap, message, stderrfile){
  if (status != 0) {
    def output = readFile(stderrfile)
    print(output)
    slackSend(channel: "#analytics-alerts", message: "*$tap:* $message \n *Reason:* $output", color: "#ff0000")
    currentBuild.result = 'FAILED'
    error "$message"
  }
}
