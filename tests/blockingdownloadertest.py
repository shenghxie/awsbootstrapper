import unittest
from mock import Mock
from blockingdownloader import BlockingDownloader
from manifest import Manifest
from instancemanager import InstanceManager
from instancemetadata import InstanceMetadata

class BlockingDownloaderTest(unittest.TestCase):

    def testConstructor(self):
        manifest = Mock(spec = Manifest)
        manifest.GetJobs.return_value = [{"Id": 3},{"Id": 100}]
        instanceManager = Mock(spec = InstanceManager)
        b= BlockingDownloader(manifest, instanceManager)
        self.assertEqual(b._manifest, manifest)
        self.assertEqual(b._instance_manager, instanceManager)
        self.assertEqual(b._allJobIds, [3,100])
        self.assertEqual(b._activeJobs, set([3,100]))

    def test_run_with_no_active_jobs(self):
        manifest = Mock(spec = Manifest)
        manifest.GetJobs.return_value = [{"Id": 3},{"Id": 100}]
        instanceManager = Mock(spec = InstanceManager)
        b= BlockingDownloader(manifest, instanceManager)
        b._activeJobs = set([])
        downloads = []
        res = b.run(downloads)
        self.assertEqual(res, {})

    def test_run_with_none_finished(self):
        manifest = Mock(spec = Manifest)
        manifest.GetJobs.return_value = [{"Id": 3},{"Id": 100}]
        #manifest.GetJob.side_effect = lambda x : [{"Id": 3},{"Id": 100}]
        instanceMetadata3 = Mock(spec=InstanceMetadata)
        instanceMetadata3.AllTasksFinished.return_value = False
        instanceMetadata100 = Mock(spec=InstanceMetadata)
        instanceMetadata100.AllTasksFinished.return_value = False
        instanceMetadata = {
            3: instanceMetadata3,
            100: instanceMetadata100
        }
        instanceManager = Mock(spec = InstanceManager)
        instanceManager.downloadMetaData.side_effect = lambda k : instanceMetadata[k]
        b = BlockingDownloader(manifest, instanceManager)
        downloads = []
        res = b.run(downloads)
        self.assertEqual(res,
            {
                3: instanceMetadata3,
                100: instanceMetadata100
            })

    def test_run_with_finished(self):
        manifest = Mock(spec = Manifest)
        manifest.GetJobs.return_value = [{"Id": 3},{"Id": 100}]
        expected_downloads = [
          {
            "Name": "document1",
            "Direction": "LocalToAWS",
            "LocalPath": ".",
            "AWSInstancePath": "awsinstancepath"
          },
          {
            "Name": "document2",
            "Direction": "AWSToLocal",
            "LocalPath": ".",
            "AWSInstancePath": "awsinstancepath1"
          },
        ]
        manifest.GetInstanceS3Documents.side_effect = lambda x : expected_downloads

        instanceMetadata3 = Mock(spec=InstanceMetadata)
        instanceMetadata3.AllTasksFinished.return_value = False
        instanceMetadata100 = Mock(spec=InstanceMetadata)
        instanceMetadata100.AllTasksFinished.return_value = True
        instanceMetadata = {
            3: instanceMetadata3,
            100: instanceMetadata100
        }
        instanceManager = Mock(spec = InstanceManager)
        instanceManager.downloadMetaData.side_effect = lambda k : instanceMetadata[k]
        b = BlockingDownloader(manifest, instanceManager)
        downloads = []
        res = b.run(downloads)
        self.assertTrue(downloads == ["document2"])
        self.assertEqual(res,
            {
                3: instanceMetadata3,
                100: instanceMetadata100
            })
        #in subsequent called to run, id 100 is removed, check
        downloads = []
        res2 = b.run(downloads)
        self.assertTrue(len(downloads) == 0)
        self.assertEqual(res2,
            {
                3: instanceMetadata3
            })

        #now make the other document finished
        downloads = []
        instanceMetadata3.AllTasksFinished.return_value = True
        res3 = b.run(downloads)
        self.assertEqual(res3,{ 3: instanceMetadata3})
        self.assertTrue(downloads == ["document2"])

        #nothing left, no downloads, no results
        downloads = []
        res4 = b.run(downloads)
        self.assertEqual(res4,{ })#all done
        self.assertTrue(len(downloads) == 0)