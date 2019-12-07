package com.atguigu.bean


object Module {

    case class QzQuestion(questionid: Int, parentid: Int, questypeid: Int, quesviewtype: Int, content: String, answer: String,
                          analysis: String, limitminute: String, var score: java.math.BigDecimal, var splitscore: java.math.BigDecimal, status: String,
                          optnum: Int, lecture: String, creator: String, createtime: String, modifystatus: String,
                          attanswer: String, questag: String, vanalysisaddr: String, difficulty: String, quesskill: String,
                          vdeoaddr: String, dt: String, dn: String)

    case class QzPoint(
                              pointid: Int, courseid: Int, pointname: String, pointyear: String, chapter: String,
                              creator: String, createtime: String, status: String, modifystatus: String, excisenum: Int,
                              pointlistid: Int, chapterid: Int, sequence: String, pointdescribe: String, pointlevel: String,
                              typelist: String, var score:java.math.BigDecimal, thought: String, remid: String, pointnamelist: String,
                              typelistids: String, pointlist: String, dt: String, dn: String
                      )


    case class QzPaperView(paperviewid: Int, paperid: Int, paperviewname: String, paperparam: String, openstatus: String,
                       explainurl: String, iscontest: String, contesttime: String, conteststarttime: String, contestendtime: String,
                       contesttimelimit: String, dayiid: Int, status: String, creator: String, createtime: String,
                       paperviewcatid: Int, modifystatus: String, description: String, papertype: String, downurl: String,
                       paperuse: String, paperdifficult: String, testreport: String, paperuseshow: String, dt: String, dn: String)

    case class QzMemberPaperQuestion(
                                            userid: Int,
                                            paperviewid: Int,
                                            chapterid: Int,
                                            sitecourseid: Int,
                                            questionid: Int,
                                            majorid: Int,
                                            useranswer: String,
                                            istrue: String,
                                            lasttime: String,
                                            opertype: String,
                                            paperid: Int,
                                            spendtime: Int,
                                            var score: java.math.BigDecimal,
                                            question_answer: Int,
                                            dt:String,
                                            dn:String
                                    )


    //所属行业数据
    case class QzBusiness(
                                 businessid: Int,
                                 businessname: String,
                                 sequence: String,
                                 status: String,
                                 creator: String,
                                 createtime: String,
                                 siteid: Int
                         )

    //主题数据
    case class QzCenter(centerid: Int,
                        centername: String,
                        centeryear: String,
                        centertype: String,
                        openstatus: String,
                        centerparam: String,
                        description: String,
                        creator: String,
                        createtime: String,
                        sequence: String,
                        provideuser: String,
                        centerviewtype: String,
                        stage: String)


    //试卷主题关联数据
    case class QzCenterPaper(
                                    paperviewid: Int,
                                    centerid: Int,
                                    openstatus: String,
                                    sequence: String,
                                    creator: String,
                                    createtime: String)

    //章节数据
    case class QzChapter(
                                chapterid: Int,
                                chapterlistid: Int,
                                chaptername: String,
                                sequence: String,
                                showstatus: String,
                                status: String,
                                chapter_creator: String,
                                chapter_createtime: String,
                                chapter_courseid: Int,
                                chapternum: Int,
                                chapterallnum: Int,
                                outchapterid: Int,
                                chapterlistname: String,
                                poIntid: Int,
                                questionid: Int,
                                questype: Int,
                                poIntname: String,
                                poIntyear: String,
                                chapter: String,
                                excisenum: Int,
                                poIntlistid: Int,
                                poIntdescribe: String,
                                poIntlevel: String,
                                typelist: String,
                                poInt_score: BigDecimal,
                                thought: String,
                                remid: String,
                                poIntnamelist: String,
                                typelistids: String,
                                poIntlist: String)


    //章节列表数据
    case class QzChapterList(
                                    chapterlistid: Int,
                                    chapterlistname: String,
                                    courseid: Int,
                                    chapterallnum: Int,
                                    sequence: String,
                                    status: String,
                                    creator: String,
                                    createtime: Long
                            )

    //题库课程数据
    case class QzCourse(
                               courseid: Int,
                               majorid: Int,
                               coursename: String,
                               coursechapter: String,
                               sequence: String,
                               isadvc: String,
                               creator: String,
                               createtime: Long,
                               status: String,
                               chapterlistid: Int,
                               poIntlistid: Int
                       )

    //课程辅导数据
    case class QzCourseEduSubject(
                                         courseeduid: Int,
                                         edusubjectid: Int,
                                         courseid: Int,
                                         creator: String,
                                         createtime: Long,
                                         majorid: Int
                                 )

    //主修数据
    case class QzMajor(majorid: Int,
                       businessid: Int,
                       siteid: Int,
                       majorname: String,
                       shortname: String,
                       status: String,
                       sequence: String,
                       creator: String,
                       createtime: Long,
                       column_sitetype: String)


}
