import unittest
from mock import Mock
from blockingdownloader import BlockingDownloader
from manifest import Manifest
from instancemanager import InstanceManager
from instancemetadata import InstanceMetadata
from s3interface import S3Interface
class BlockingDownloaderTest(unittest.TestCase):

    def testConstructor(self):
        manifest = Mock(spec = Manifest)
        manifest.GetJobs.return_value = [{"Id": 3},{"Id": 100}]
        instanceManager = Mock(spec = InstanceManager)
        s3interface = Mock(spec = S3Interface)
        b= BlockingDownloader(manifest, instanceManager, s3interface)
        self.assertEqual(b._manifest, manifest)
        self.assertEqual(b._instance_manager, instanceManager)
        self.assertEqual(b._s3interface, s3interface)
        self.assertEqual(b._allJobIds, [3,100])
        self.assertEqual(b._activeJobs, set([3,100]))

    def test_run_with_no_active_jobs(self):
        manifest = Mock(spec = Manifest)
        manifest.GetJobs.return_value = [{"Id": 3},{"Id": 100}]
        instanceManager = Mock(spec = InstanceManager)
        s3interface = Mock(spec = S3Interface)
        b= BlockingDownloader(manifest, instanceManager, s3interface)
        b._activeJobs = set([])
        res = b.run()
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
        s3interface = Mock(spec = S3Interface)
        b = BlockingDownloader(manifest, instanceManager, s3interface)
        res = b.run()
        self.assertEqual(res,
            {
                3: instanceMetadata3,
                100: instanceMetadata100
            })

    def test_run_with_finished(self):
        manifest = Mock(spec = Manifest)
        manifest.GetJobs.return_value = [{"Id": 3},{"Id": 100}]
        manifest.GetInstanceS3Documents.side_effect = lambda x : []

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
        s3interface = Mock(spec = S3Interface)
        b = BlockingDownloader(manifest, instanceManager, s3interface)
        res = b.run()
        self.assertEqual(res,
            {
                3: instanceMetadata3,
                100: instanceMetadata100
            })
        #in subsequent called to run, id 100 is removed, check
        res2 = b.run()
        self.assertEqual(res2,
            {
                3: instanceMetadata3
            })
