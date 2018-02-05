pipeline {
  agent any
  options {
    buildDiscarder(logRotator(numToKeepStr: '5'))
    properties([
  pipelineTriggers([
    issueCommentTrigger('.*test this please.*')
  ])
])
  }
  stages {
    // Build stage
    stage('Build Core Image') {
      steps {
        sh "docker build -t gangacoretest:${env.BRANCH_NAME}-${env.BUILD_ID} -f ${env.WORKSPACE}/python/GangaCore/test/Dockerfile ."
      }
    }
    // Parallel testing stage
    stage('Test') {
      parallel {
        stage('GangaCore') {
          steps {
            sh "docker run --name GangaCore${env.BRANCH_NAME}-${env.BUILD_ID} gangacoretest:${env.BRANCH_NAME}-${env.BUILD_ID} || true"
            sh "docker cp GangaCore${env.BRANCH_NAME}-${env.BUILD_ID}:/root/tests-GangaCore.xml ."
            sh "docker cp GangaCore${env.BRANCH_NAME}-${env.BUILD_ID}:/root/cov-GangaCore.xml ."
          }
          post {
            always {
              junit "**/tests-GangaCore.xml"
              sh "docker rm --force GangaCore${env.BRANCH_NAME}-${env.BUILD_ID}" 
            }
          }
        }
        stage('GangaDirac') {
          steps {
            sh "docker build -t gangadiractest:${env.BRANCH_NAME}-${env.BUILD_ID} -f ${env.WORKSPACE}/python/GangaDirac/test/Dockerfile ."
            sh "docker run --name GangaDirac${env.BRANCH_NAME}-${env.BUILD_ID} -v ~/.globus:/root/.globus -e vo=gridpp gangadiractest:${env.BRANCH_NAME}-${env.BUILD_ID} || true"
            sh "docker cp GangaDirac${env.BRANCH_NAME}-${env.BUILD_ID}:/root/tests-GangaDirac.xml ."
            sh "docker cp GangaDirac${env.BRANCH_NAME}-${env.BUILD_ID}:/root/cov-GangaDirac.xml ."
          }
          post {
            always {
              junit "**/tests-GangaDirac.xml"
              sh "docker rm --force GangaDirac${env.BRANCH_NAME}-${env.BUILD_ID}"
              sh "docker rmi --force gangadiractest:${env.BRANCH_NAME}-${env.BUILD_ID}"
            }
          }
        }
        stage('GangaLHCb') {
          steps {
            sh "docker build -t gangalhcbtest:${env.BRANCH_NAME}-${env.BUILD_ID} -f ${env.WORKSPACE}/python/GangaLHCb/test/Dockerfile ."
            sh "docker run --privileged --name GangaLHCb${env.BRANCH_NAME}-${env.BUILD_ID} -v ~/.globus:/root/.globus gangalhcbtest:${env.BRANCH_NAME}-${env.BUILD_ID} || true"
            sh "docker cp GangaLHCb${env.BRANCH_NAME}-${env.BUILD_ID}:/root/tests-GangaLHCb.xml ."
            sh "docker cp GangaLHCb${env.BRANCH_NAME}-${env.BUILD_ID}:/root/cov-GangaLHCb.xml ."
          }
          post {
            always {
              junit "**/tests-GangaLHCb.xml"
              sh "docker rm --force GangaLHCb${env.BRANCH_NAME}-${env.BUILD_ID}"
              sh "docker rmi --force gangalhcbtest:${env.BRANCH_NAME}-${env.BUILD_ID}"
            }
          }
        }
      } // end parallel
    }
  }
  post { 
    always { 
      cobertura autoUpdateHealth: false, autoUpdateStability: false, coberturaReportFile: 'cov-*.xml', conditionalCoverageTargets: '70, 0, 0', failUnhealthy: false, failUnstable: false, lineCoverageTargets: '80, 0, 0', maxNumberOfBuilds: 0, methodCoverageTargets: '80, 0, 0', onlyStable: false, sourceEncoding: 'ASCII', zoomCoverageChart: false
      sh "docker rmi --force gangacoretest:${env.BRANCH_NAME}-${env.BUILD_ID}"
    }
  }
}
