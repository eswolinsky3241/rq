from time import sleep

from rq import Queue, SimpleWorker
from rq.batch import Batch
from rq.exceptions import NoSuchBatchError
from rq.utils import as_text
from tests import RQTestCase
from tests.fixtures import say_hello


class TestBatch(RQTestCase):
    job_1_data = Queue.prepare_data(say_hello, job_id='job1')
    job_2_data = Queue.prepare_data(say_hello, job_id='job2')

    def test_create_batch(self):
        q = Queue(connection=self.testconn)
        batch = q.enqueue_many([self.job_1_data, self.job_2_data], batch=True)
        assert isinstance(batch, Batch)
        assert len(batch.jobs) == 2
        q.empty

    def test_batch_jobs(self):
        q = Queue(connection=self.testconn)
        batch = q.enqueue_many([self.job_1_data, self.job_2_data], batch=True)
        jobs = q.enqueue_many([self.job_1_data, self.job_2_data], batch=False)
        self.assertCountEqual(batch.jobs, jobs)
        q.empty()

    def test_fetch_batch(self):
        q = Queue(connection=self.testconn)
        enqueued_batch = q.enqueue_many([self.job_1_data, self.job_2_data], batch=True)
        fetched_batch = Batch.fetch(enqueued_batch.id, self.testconn)
        self.assertCountEqual(enqueued_batch.jobs, fetched_batch.jobs)
        assert len(fetched_batch.jobs) == 2
        q.empty()

    def test_add_jobs(self):
        q = Queue(connection=self.testconn)
        batch = q.enqueue_many([self.job_1_data], batch=True)
        job = q.enqueue_many([self.job_2_data], batch=False)
        batch.add_jobs(job)
        assert job[0] in batch.jobs
        self.assertEqual(job[0].batch_id, batch.id)
        q.empty()

    def test_jobs_added_to_batch_key(self):
        q = Queue(connection=self.testconn)
        batch = q.enqueue_many([self.job_1_data, self.job_2_data], batch=True)
        job_ids = [job.id for job in batch.jobs]
        jobs = list({as_text(job) for job in self.testconn.smembers(batch.key)})
        self.assertCountEqual(jobs, job_ids)
        q.empty()

    def test_deleted_jobs_removed_from_batch(self):
        q = Queue(connection=self.testconn)
        batch = q.enqueue_many([self.job_1_data, self.job_2_data], batch=True)
        job = batch.jobs[0]
        job.delete()
        batch.refresh()
        redis_jobs = list({as_text(job) for job in self.testconn.smembers(batch.key)})
        assert job.id not in redis_jobs
        assert job not in batch.jobs

    def test_batch_added_to_registry(self):
        q = Queue(connection=self.testconn)
        batch = q.enqueue_many([self.job_1_data], batch=True)
        redis_batches = {as_text(batch) for batch in self.testconn.smembers("rq:batches")}
        assert batch.id in redis_batches
        q.empty()

    def test_expired_jobs_removed_from_batch(self):
        q = Queue(connection=self.testconn)
        w = SimpleWorker([q], connection=q.connection)
        short_lived_job = Queue.prepare_data(say_hello, result_ttl=1)
        batch = q.enqueue_many([short_lived_job, self.job_1_data], batch=True)
        w.work(burst=True, max_jobs=1)
        sleep(3)
        batch.refresh()
        assert len(batch.jobs) == 1
        q.empty()

    def test_empty_batch_removed_from_batch_list(self):
        q = Queue(connection=self.testconn)
        w = SimpleWorker([q], connection=q.connection)
        short_lived_job = Queue.prepare_data(say_hello, result_ttl=1)
        batch = q.enqueue_many([short_lived_job], batch=True)
        w.work(burst=True, max_jobs=1)
        sleep(3)
        w.run_maintenance_tasks()
        redis_batches = {as_text(batch) for batch in self.testconn.smembers("rq:batches")}
        assert batch.id not in redis_batches

    def test_fetch_expired_batch_raises_error(self):
        q = Queue(connection=self.testconn)
        w = SimpleWorker([q], connection=q.connection)
        short_lived_job = Queue.prepare_data(say_hello, result_ttl=1)
        batch = q.enqueue_many([short_lived_job], batch=True)
        w.work(burst=True, max_jobs=1)
        sleep(3)
        self.assertRaises(NoSuchBatchError, Batch.fetch, batch.id, batch.connection)
        q.empty()
