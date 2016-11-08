import unittest
from ec2interface import EC2Interface

class MockEC2Instance(object):
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

    def __init__(self):
        self.create_instances_func = lambda **args : None

    def create_instances(self, **args):
        return self.create_instances_func(**args)

class MockManifest(object):
    
    def __init__(self):
        self.getjobsOverride = lambda: None
        self.GetBucketNameOverride = lambda : None
        self.GetS3KeyPrefixOverride = lambda : None

    def GetJobs(self):
        return self.getjobsOverride()
    
    def GetBucketName(self):
        return self.GetBucketNameOverride()

    def GetS3KeyPrefix(self):
        return self.GetS3KeyPrefixOverride()

class MockInstanceManager(object):

    def __init__(self):
        self.publishinstanceoverride = lambda id, instance: None

    def publishInstance(self, id, instance):
        return self.publishinstanceoverride(id, instance)

class EC2Interface_Test(unittest.TestCase):
    
    def test_constructor(self):
        ec2Resource = MockEC2Resource()
        manifest = MockManifest()
        instanceManager = MockInstanceManager()

        ec2interface = EC2Interface(
            ec2Resource=ec2Resource, 
            instanceLocalWorkingDir = "instanceLocalWorkingDir", 
            manifest = manifest, 
            manifestKey = "key/to/manifest",
            instanceManager = instanceManager,
            bootstrapScriptPath = "\path\to\script.py",
            bootstrapCommandFormat = "{0}\n")

        self.assertEqual(ec2interface.ec2Resource, ec2Resource)
        self.assertEqual(ec2interface.instanceLocalWorkingDir, "instanceLocalWorkingDir")
        self.assertEqual(ec2interface.manifest, manifest)
        self.assertEqual(ec2interface.manifestKey, "key/to/manifest")
        self.assertEqual(ec2interface.instanceManager, instanceManager)
        self.assertEqual(ec2interface.bootstrapScriptPath, "\path\to\script.py")
        self.assertEqual(ec2interface.bootstrapCommandFormat, "{0}\n")

    def test_buildBootstrapCommand(self):
        ec2Resource = MockEC2Resource()
        manifest = MockManifest()

        def GetBucketNameOverride():
            return "bucket"

        manifest.GetBucketNameOverride = GetBucketNameOverride

        instanceManager = MockInstanceManager()

        ec2interface = EC2Interface(
            ec2Resource=ec2Resource, 
            instanceLocalWorkingDir = "instanceLocalWorkingDir", 
            manifest = manifest, 
            manifestKey = "key/to/manifest",
            instanceManager = instanceManager,
            bootstrapScriptPath = "\path\to\script.py",
            bootstrapCommandFormat = "{0}\nextraCommand")

        result = ec2interface.buildBootstrapCommand(1)
        self.assertEqual(result,
                       "'\path\to\script.py' "+
                       "--bucketName bucket "+
                       "--manifestKey key/to/manifest "+
                       "--instanceId 1 "+
                       "--localWorkingDir 'instanceLocalWorkingDir'" +
                       "\nextraCommand")

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

        def create_instances(**args):
            i = MockEC2Instance()
            for k,v in self.args.iteritems():
                if k != "UserData":
                    self.assertTrue(args[k] == v)
            return [i]

        ec2Resource.create_instances_func = create_instances

        manifest = MockManifest()

        def GetBucketNameOverride():
            return "bucket"
        manifest.GetBucketNameOverride = GetBucketNameOverride
        def GetS3KeyPrefix():
            return "prefix"
        manifest.GetS3KeyPrefixOverride = GetS3KeyPrefix

        def GetJobs():
            return [ {"Id": 1}, {"Id": 2}, {"Id": 3} ]
        manifest.getjobsOverride = GetJobs

        instanceManager = MockInstanceManager()
        def publishInstance(id, instance):
            self.assertTrue(instance.waitCalled)
            self.assertTrue(instance.Tags[0]["Key"] == "Name")
            self.assertTrue(instance.Tags[0]["Value"] == "{0}[{1}]".format(GetS3KeyPrefix(),id))

        instanceManager.publishinstanceoverride = publishInstance

        ec2interface = EC2Interface(
            ec2Resource=ec2Resource, 
            instanceLocalWorkingDir = "instanceLocalWorkingDir", 
            manifest = manifest, 
            manifestKey = "key/to/manifest",
            instanceManager = instanceManager,
            bootstrapScriptPath = "\path\to\script.py",
            bootstrapCommandFormat = "{0}\nextraCommand")


        ec2interface.launchInstances(self.args)


if __name__ == '__main__':
    unittest.main()
