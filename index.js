'use strict'
const { v4: uuidv4 } = require('uuid')
module.exports = class QueWrapper {
  constructor(opts = {}) {
    this.Queue = require('bull')
    this.uuidv4 = uuidv4
    this.que = new this.Queue(opts.queName, opts.queOptions)
    this.log = {
      info: this.logInfo,
      error: this.logError,
      debug: this.logInfo
    }
    if(opts.logger) this.log = opts.logger
    this.localQue = opts.localQue
    this.localQueKey = opts.localQueKey
    this.defaultJobOpts = {removeOnComplete: true, removeOnFail: true, attempts: 1, timeout: 600000}
    if(opts.defaultJobOpts) this.defaultJobOpts = {...this.defaultJobOpts, ...opts.defaultJobOpts}
    this.cmdProcessor = opts.cmdProcessor
    if(!this.cmdProcessor) this.cmdProcessor = this.defaultProcessor
    this.opts = opts
    if(!this.opts.numJobs) this.opts.numJobs = 3
    if(this.opts.createListeners) this.createListeners()
  }
  logInfo (msg){
    console.log(msg)
  }
  logError (msg){
    console.log(msg)
  }
  defaultProcessor(obj = {}){
    try{
      this.log.error('There is no processor for '+obj?.data?.name)
    }catch(e){
      thore(e);
    }
  }
  process ()  {
    this.log.info('starting '+this.opts.queName+' processing with '+this.opts.numJobs+' workers')
    this.que.process('*', this.opts.numJobs, (job)=>{
      return new Promise(async(resolve, reject)=>{
        try{
          const res = {status: 'no job data'}
          if(job?.data){
            res.status = 'error getting job'
            const obj = await this.addtoLocalQue(job)
    				if(obj){
              res.status = 'complete'
              await this.cmdProcessor(obj);
            }
    			}
    			resolve(res)
        }catch(e){
          reject(e);
        }
      })
    })
  }
  async addtoLocalQue (job) {
    try{
      const obj = JSON.parse(JSON.stringify(job.data))
      obj.timestamp = job.timestamp
      obj.jobId = job?.opts?.jobId
      if(!obj.id) obj.id = obj.jobId
      if(this.localQue && this.localQueKey) await this.localQue.setTTL(this.localQueKey+'-'+obj.jobId, obj, 600)
      return obj
    }catch(e){
      throw(e);
    }
  }
  async start (){
    try{
      if(this.localQue && this.localQueKey) await this.processLocalQue(this.que)
      this.process()
    }catch(e){
      this.log.error(e);
      setTimeout(this.start, 500)
    }
  }
  async processLocalQue (){
    try{
      if(!this.localQue || !this.localQueKey) return;
      let count = 0, failed = 0
      const jobs = await this.localQue.keys(this.localQueKey+'-*')
      if(jobs.length > 0){
        let timeNow = Date.now()
        timeNow = +timeNow - 599999
        for(let i in jobs){
          const obj = await this.localQue.get(jobs[i])
          if(obj && obj.timestamp > timeNow){
            count++
            await this.cmdProcessor(obj)
          }else{
            failed++
          }
          await this.localQue.del(jobs[i])
          if(obj.jobId) await this.removeJob(obj.jobId)
        }
      }
      this.log.info('Processed '+count+' left over in '+this.opts.queName+' job que. Deleted '+failed+' invalid')
    }catch(e){
      this.log.error(e);
    }
  }
  async add (data, opts){
    try{
      const job = await this.newJob(data, opts)
      return job?.finished()
    }catch(e){
      throw(e);
    }
  }
  async newJob (data, opts){
    try{
      let jobOptions = JSON.parse(JSON.stringify(this.defaultJobOpts))
      if(opts) jobOptions = {...jobOptions, ...opts}
      if(!jobOptions.jobId) jobOptions.jobId = await this.uuidv4()
      const job = await this.que.add(this.opts.queName, data, jobOptions)
      return job
    }catch(e){
      throw(e);
    }
  }
  async getJob (jobId){
    try{
      return await this.que.getJob(jobId)
    }catch(e){
      throw(e);
    }
  }
  async getCompleted (){
    try{
      return await this.que.getCompleted()
    }catch(e){
      throw(e);
    }
  }
  async getJobs (){
    try{
      return await this.que.getJobs()
    }catch(e){
      throw(e);
    }
  }
  async removeJob (jobId) {
    try{
      const job = await this.que.getJob(jobId)
      if(job){
        await job.moveToCompleted(null, true, true)
        await job.remove()
      }
    }catch(e){
      //console.error(e);
    }
  }
  createListeners (){
    this.log.info('Creating '+this.opts.queName+' que listeners...')
    this.que.on('global:failed', function (jobId, err) {
			this.log.info(`Job ${jobId} failed with reason: ${err}`)
			// A job failed with reason `err`!
		})
		this.que.on('global:error', (error)=>{
			this.log.error(error);
		})
  }
}
