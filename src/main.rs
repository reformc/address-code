use reqwest;
use scraper::{Html, Selector};
use regex::Regex;
use tokio::sync::mpsc;
use std::{fs::OpenOptions, io::Read};
use std::io::{Write, Seek};
use serde::{Deserialize, Serialize};
use serde_json;
use tokio::signal;
use clap::Parser;

//const FILEPATH:&str = "G:/area_code.txt";
//const TMPFILE:&str = "G:/url_list.txt";
const INDEXADDR:&str = "http://www.stats.gov.cn/tjsj/tjbz/tjyqhdmhcxhfdm/2021/";
static GRADE:[&str;5] = ["province","city","county","town","village"];

#[derive(Parser, Debug)]
#[clap(
    author="reform <reformgg@gmail.com>", 
    version="0.1.0",
    about="五级行政代码爬虫",
    long_about = "
    五级行政地址代码爬虫,
    首页地址:
    http://www.stats.gov.cn/tjsj/tjbz/tjyqhdmhcxhfdm/2021/
    "
)]
struct Args {
    ///缓存文件。
    #[clap(long,short,default_value = "url_list.txt")]
    tmp:String,
    ///行政区域代码文件
    #[clap(long,short,default_value = "area_code.txt")]
    file:String,
    ///每次爬取间隔(秒)
    #[clap(long,short,default_value = "3")]
    duration:u64
}

#[derive(Debug, Serialize, Deserialize)]
struct UrlByGrade{
    grade:usize,
    url_str:String,
    code:String,
    addr:Vec<String>,
    classify:Option<String>,
}

#[tokio::main]
async fn main() {
    run().await;
    println!("Hello, world!");
}

async fn ctrl_c(tx:mpsc::Sender<u8>){
    signal::ctrl_c().await.unwrap();
    tx.send(0).await.unwrap();
}

async fn run(){
    let args = Args::parse();
    let url_tmp = Queue::new(&args.tmp);
    url_tmp.init();
    let (tx, mut rx) = mpsc::channel(2);
    tokio::spawn(ctrl_c(tx));
    while let (index,Some(msg))=url_tmp.pull(){
        if let Ok(_) = rx.try_recv(){
            println!("缓存完毕，程序退出");
            std::process::exit(0);
        }
        println!("{},{:?}",&msg.code,&msg.addr);
        tokio::time::sleep(tokio::time::Duration::from_secs(args.duration)).await;
        match msg.grade{
            0=>{                
                match province(&msg.url_str.clone(), msg.grade, &msg.addr).await{
                    Ok(res)=>{
                        url_tmp.push(res);
                        url_tmp.pull_del(index)
                    },
                    Err(e)=>{
                        println!("{}",e);
                    }
                };
            },
            1|2|3=>{
                //此段代码为测试用
                //if msg.code !="330000000000"&&msg.code!= "330100000000"&&msg.code!= "330110000000"&&msg.code!= "330110005000"{
                //    continue
                //}
                match mid(&args.file,&msg.url_str.clone(), msg.grade, &msg.addr).await{
                    Ok(res)=>{                        
                        url_tmp.push(res);
                        url_tmp.pull_del(index)
                    },
                    Err(e)=>{
                        println!("{}",e);
                    }
                };
            },
            4=>{
                match village(&msg.url_str.clone(), msg.grade, &msg.addr).await{
                    Ok(res)=>{
                        for c in res{
                            write(&args.file,c);
                        }
                        url_tmp.pull_del(index);
                    }
                    Err(e)=>{
                        println!("{}",e);
                    }
                }
            }
            _=>{}
        }
    }
}

fn write(file_path:&str,cell:UrlByGrade){
    let mut file = OpenOptions::new().create(true).append(true).open(file_path).expect("cannot open file");
    let b = &format!("code:{}\tclassify:{}\taddr:{:?}\n",cell.code,cell.classify.unwrap_or("".to_string()),cell.addr);
    file.write_all(b.as_bytes()).unwrap();
}

async fn province(url:&str,grade:usize,superior:&Vec<String>)->Result<Vec<UrlByGrade>,reqwest::Error>{
    let re = Regex::new(r"/\d*.html").unwrap();
    let host = re.replace(url, "/");
    let resp = reqwest::get(url).await?;
    let body = resp.text().await?;
    //println!("Body:{}",body);
    let doc = Html::parse_fragment(&body);
    let selector_province = Selector::parse(&format!("table.{}table",GRADE[grade])).unwrap();
    let doc = doc.select(&selector_province).last().unwrap();
    let mut res = vec!();
    let selector = Selector::parse("a").unwrap();
    for el in doc.select(&selector) {
        let mut addr = superior.clone();
        let path = el.value().attr("href").unwrap();
        let code = path.replace(".html", "0000000000");
        addr.push(el.inner_html().replace("<br>", ""));
        res.push(UrlByGrade{
            grade:grade+1,
            url_str:format!("{}{}",host,path),
            code,
            addr,
            classify:None
        });
    };    
    Ok(res)
}

async fn mid(code_file_path:&str,url:&str,grade:usize,superior:&Vec<String>)->Result<Vec<UrlByGrade>,reqwest::Error>{
    println!("{},{}",url,grade);
    let re = Regex::new(r"/\d*.html").unwrap();
    let host = re.replace(url, "/");
    let resp = reqwest::get(url).await?;
    let body = resp.text().await?;
    //println!("Body:{}",body);
    let doc = Html::parse_fragment(&body);
    let selector_grade = Selector::parse(&format!("table.{}table",GRADE[grade])).unwrap();
    let doc = doc.select(&selector_grade).last().expect(&format!("{}{}",url,GRADE[grade]));
    let mut res = vec!();
    let selector = Selector::parse(&format!("tr.{}tr",GRADE[grade])).unwrap();
    for el in doc.select(&selector) {
        //println!("{}",el.inner_html());
        let mut addr = superior.clone();
        let mut code = "".to_string();
        let mut url_str_o = Some("".to_string());
        let re_code = Regex::new(r"^\d*$").unwrap();
        for td in el.select(&(Selector::parse("td").unwrap())){
            match td.select(&(Selector::parse("a").unwrap())).last(){//部分市辖区没有下一级
                Some(a)=>{  
                    let body = a.inner_html();
                    if re_code.is_match(&body){
                        code = body;
                    }else{
                        addr.push(body);
                    }
                    url_str_o = Some(format!("{}{}",host,a.value().attr("href").unwrap()));
                },
                None=>{
                    let body = td.inner_html();
                    if re_code.is_match(&body){
                        code = body;
                    }else{
                        addr.push(body)
                    };
                    url_str_o = None;
                }
            };
        };  
        //println!("{}",url_str.clone().unwrap());
        match url_str_o{
            Some(url_str)=>{
                res.push(UrlByGrade{
                    grade:grade+1,
                    url_str,
                    code,
                    addr,
                    classify:None
                });
            }
            None=>{
                write(code_file_path,UrlByGrade{
                    grade:grade+1,
                    url_str:"".to_string(),
                    code,
                    addr,
                    classify:None
                });
            }
        } 
        
    }
    Ok(res)
}

async fn village(url:&str,grade:usize,superior:&Vec<String>)->Result<Vec<UrlByGrade>,reqwest::Error>{
    let resp = reqwest::get(url).await?;
    let body = resp.text().await?;
    //println!("Body:{}",body);
    let doc = Html::parse_fragment(&body);
    let selector_grade = Selector::parse(&format!("table.{}table",GRADE[grade])).unwrap();    
    let doc = doc.select(&selector_grade).last().expect(&format!("{}{}",url,GRADE[grade]));
    let mut res = vec!();
    let selector = Selector::parse(&format!("tr.{}tr",GRADE[grade])).unwrap();
    for el in doc.select(&selector) {
        let mut addr = superior.clone();
        let mut array =vec!();
        for td in el.select(&(Selector::parse("td").unwrap())){
            array.push(td.inner_html());
        }
        addr.push(array[2].clone());
        res.push(UrlByGrade{
            grade:grade+1,
            url_str:"last_grade".to_string(),
            code:array[0].clone(),
            addr,
            classify:Some(array[1].clone())
        });
    }
    Ok(res)
}

struct Queue{
    file_path:String,
}

impl Queue{
    fn new(file_path:&str)->Queue{
        Queue {
            file_path:file_path.to_string()
        }
    }

    fn init(&self){
        match std::fs::File::open(&self.file_path){
            Ok(mut f)=>{
                let mut contents = String::new();
                f.read_to_string(&mut contents).unwrap();
                let list:Vec<UrlByGrade> = serde_json::from_str(&contents).unwrap_or(vec!());
                if list.len()<1{
                    println!("缓存列表已为空，如需重新爬取，请删除缓存文件重新运行");
                    std::process::exit(0);
                }
            },
            Err(_)=>{
                println!("缓存文件不存在，重新爬取");
                let mut f = std::fs::File::create(&self.file_path).expect("文件创建失败");
                let data = serde_json::to_string_pretty(&vec!(UrlByGrade{
                    grade:0,
                    url_str:INDEXADDR.to_string(),
                    code:0.to_string(),
                    addr:vec!(),
                    classify:None
                })).unwrap();
                f.write_all(data.as_bytes()).unwrap();
            }
        };
    }

    fn pull(&self)->(usize,Option<UrlByGrade>){
        let mut f = std::fs::File::open(&self.file_path).unwrap();
        let mut contents = String::new();
        f.read_to_string(&mut contents).unwrap();
        let mut list:Vec<UrlByGrade> = serde_json::from_str(&contents).unwrap();
        let len = list.len();
        match len {
            0 => (len-1,None),
            _ => (len-1,Some(list.remove(len-1))),
        }
    }

    fn pull_del(&self,index:usize){
        let mut f = std::fs::OpenOptions::new().read(true).write(true).open(&self.file_path).unwrap();
        let mut contents = String::new();
        f.read_to_string(&mut contents).unwrap();
        let mut list:Vec<UrlByGrade> = serde_json::from_str(&contents).unwrap();
        list.remove(index);
        let data = serde_json::to_string_pretty(&list).unwrap();
        f.set_len(0).unwrap();
        f.rewind().unwrap();
        f.write_all(data.as_bytes()).unwrap();
    }

    fn push(&self,mut resp_list:Vec<UrlByGrade>){
        let mut f = std::fs::OpenOptions::new().read(true).write(true).open(&self.file_path).unwrap();
        let mut contents = String::new();
        f.read_to_string(&mut contents).unwrap();
        let mut list:Vec<UrlByGrade> = serde_json::from_str(&contents).unwrap();
        list.append(& mut resp_list);
        let data = serde_json::to_string_pretty(&list).unwrap();
        f.set_len(0).unwrap();
        f.rewind().unwrap();
        f.write(data.as_bytes()).unwrap();
    }
}
