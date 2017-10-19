import unittest
from ec2interface import EC2Interface
from manifest import Manifest
from instancemanager import InstanceManager
from mock import Mock, call

class MockEC2Instance(object):
    '''
    boto3 objects runtime types and not mockable directly
    '''
    def __init__(self):
        self.wait_until_running_override = lambda : None
        self.create_tags_override = lambda Tags: None
        self.instance_id = 1

    def wait_until_running(self):
        self.waitCalled = True
        return self.wait_until_running_override()

    def create_tags(self, Tags):
        self.Tags = Tags
        return self.create_tags_override(Tags)

class MockEC2Resource(object):
    '''
    boto3 objects runtime types and not mockable directly
    '''
    def __init__(self):
        self.create_instances_func = lambda **args : None

    def create_instances(self, **args):
        return self.create_instances_func(**args)

class EC2Interface_Test(unittest.TestCase):
    
    def test_constructor(self):
        ec2Resource = MockEC2Resource()
        manifest = Mock(spec = Manifest)
        instanceManager = Mock(spec = InstanceManager)

        ec2interface = EC2Interface(
            ec2Resource=ec2Resource, 
            instanceLocalWorkingDir = "instanceLocalWorkingDir", 
            manifest = manifest, 
            manifestKey = "key/to/manifest",
            instanceManager = instanceManager,
            pythonpath="pythonpath",
            bootstrapScriptPath = "/path/to/script.py",
            lineBreak="\r\n",
            bootstrapCommands = ["a","b","c"])

        self.assertEqual(ec2interface.ec2Resource, ec2Resource)
        self.assertEqual(ec2interface.instanceLocalWorkingDir, "instanceLocalWorkingDir")
        self.assertEqual(ec2interface.manifest, manifest)
        self.assertEqual(ec2interface.manifestKey, "key/to/manifest")
        self.assertEqual(ec2interface.instanceManager, instanceManager)
        self.assertEqual(ec2interface.pythonpath, "pythonpath")
        self.assertEqual(ec2interface.bootstrapScriptPath, "/path/to/script.py")
        self.assertEqual(ec2interface.lineBreak, "\r\n")
        self.assertEqual(ec2interface.bootstrapCommands, ["a","b","c"])

    def test_buildBootstrapCommand(self):
        ec2Resource = MockEC2Resource()
        manifest = Mock(spec = Manifest)
        instanceManager = Mock(spec = InstanceManager)
        def GetBucketNameOverride():
            return "bucket"

        manifest.GetBucketName.side_effect = lambda: "bucket"

        ec2interface = EC2Interface(
            ec2Resource=ec2Resource, 
            instanceLocalWorkingDir = "instanceLocalWorkingDir", 
            manifest = manifest, 
            manifestKey = "key/to/manifest",
            instanceManager = instanceManager,
            pythonpath="pythonpath",
            bootstrapScriptPath = "/path/to/script.py",
            lineBreak = "\n",
            bootstrapCommands = ["$BootStrapScript", "extraCommand"])

        result = ec2interface.buildBootstrapCommand(1000)
        self.assertEqual(result,
                       'pythonpath "/path/to/script.py" '+
                       '--bucketName "bucket" '+
                       '--manifestKey "key/to/manifest" '+
                       '--instanceId 1000 '+
                       '--localWorkingDir "instanceLocalWorkingDir"' +
                       '\nextraCommand')
        result = ec2interface.buildBootstrapCommand(1001)
        self.assertEqual(result,
                       'pythonpath "/path/to/script.py" '+
                       '--bucketName "bucket" '+
                       '--manifestKey "key/to/manifest" '+
                       '--instanceId 1001 '+
                       '--localWorkingDir "instanceLocalWorkingDir"' +
                       '\nextraCommand')

    def test_launchInstances(self):
        ec2Resource = MockEC2Resource()
        self.args = {
                "ImageId": "abcd",
                "MinCount": 1,
                "MaxCount": 2,
                "UserData": "userdata",
                "SecurityGroups": "securityGroups",
                "InstanceType": "instanceType",
                "IamInstanceProfile": "IamInstanceProfile",
                "InstanceInitiatedShutdownBehavior": "InstanceInitiatedShutdownBehavior"
        }

        def create_tags_override(Tags):
            self.assertTrue(Tags[0]["Key"] == "Name")
            self.assertTrue(Tags[0]["Value"] in ["prefix[1]","prefix[2]","prefix[3]"])
        
        def create_instances(**args):
            i = MockEC2Instance()
            i.create_tags_override = create_tags_override
            for k,v in self.args.iteritems():
                if k != "UserData":
                    self.assertTrue(args[k] == v)
            return [i]

        ec2Resource.create_instances_func = create_instances

        manifest = Mock(spec = Manifest)

        manifest.GetBucketName.side_effect = lambda : "bucket"
        manifest.GetS3KeyPrefix.side_effect = lambda : "prefix"

        manifest.GetJobs.side_effect = lambda : [ {"Id": 1}, {"Id": 2}, {"Id": 3} ]

        instanceManager = Mock(spec = InstanceManager)

        instanceManager.publishInstance.side_effect = lambda id, aws_instance_id : (
            self.assertTrue(id in [1,2,3]),
            self.assertTrue(aws_instance_id == 1) #instance.Tags == [{'Key': 'Name', 'Value': 'prefix[{0}]'.format(id)}])
        )

        ec2interface = EC2Interface(
            ec2Resource=ec2Resource, 
            instanceLocalWorkingDir = "instanceLocalWorkingDir", 
            manifest = manifest, 
            manifestKey = "key/to/manifest",
            instanceManager = instanceManager,
            pythonpath="pythonpath",
            bootstrapScriptPath = "/path/to/script.py",
            lineBreak = "\n",
            bootstrapCommands = ["$BootStrapScript", "extraCommand"])

        ec2interface.launchInstances(self.args)
        instanceManager.publishInstance.assert_called()
        manifest.GetJobs.assert_called()

if __name__ == '__main__':
    unittest.main()
