import random
import pandas as pd
import pyomo.environ as pe
import pyomo.gdp as pyogdp
import os
import logging
from amplpy import modules
import psycopg2

from configparser import ConfigParser
from db_config import PostgresConnector


#Create aa=n account in neos and give the email id here in case the optimizer needs to be run in Neos
os.environ['NEOS_EMAIL'] = 'mailshubhamk@gmail.com'

logging.basicConfig(format='%(asctime)s - %(message)s',level=logging.INFO)

##TODO Index all the dataframes
class classScheduler:

    def __init__(self):
        """
        Read the input data required for running the scheduler from the database
        Update the database credentials in config.ini file
        """
        self.postgres_connector = PostgresConnector()
        logging.info("Scheduler: Connecting to postgres db.....")
        self.postgres_tables=self.postgres_connector.connect_to_postgres()
        logging.info("Scheduler: Connection established")
        self.tables_dataframes = {}
        tables_names_req=['professors','courses','professor_course_allocations','classroom','availability']
        for table_name in tables_names_req:
            if table_name in self.postgres_tables:
                #print(table_name)
                self.tables_dataframes[table_name]=self.postgres_tables[table_name]
                logging.info("Scheduler: Table "+table_name+' read successfully')
                #print(self.tables_dataframes[table_name])
            else:
             print(table_name+' table not found')
             logging.error("Scheduler:"+table_name+'not read')

        self.df_prof=self.tables_dataframes['professors']
        #print(self.df_prof)
        self.df_courses=self.tables_dataframes['courses']
        self.df_courses["contact_hours"]=pd.to_numeric(self.df_courses["contact_hours"])
        #print(self.df_courses)
        self.df_class = self.tables_dataframes['classroom']
        #print(self.df_class)
        self.df_availability=self.tables_dataframes['availability']



        logging.info("Scheduler: Input files read successfully")

        self.model = self.create_model()



    def distance_btw_classes(self):
        dist = {}
        for cls in self.df_class['id']:
            for cls2 in self.df_class['id']:
                if cls!=cls2:
                    dist[cls,cls2]=random.randint(1,10)
                else:
                    dist[cls,cls2]=0

        logging.info("Scheduler: Distance between classes computed successfully")
        return dist
        #TODO Make it actual distance

    #The distance between classroom and prof's office
    def distance_btw_classoff(self):
        disto={}
        for prof in self.df_prof['id']:
            for cls in self.df_class['id']:
                disto[prof,cls]=random.randint(1,5)
        logging.info("Scheduler: Distance between classes and offices computed successfully")
        return disto

    # Binary matrix to identify courses taken by a Prof
    def prof_courses(self):
        prof_sel=dict()
        course_alloc=self.tables_dataframes['professor_course_allocations'].copy()
        course_alloc.set_index('course_id',inplace=True)
        for course in course_alloc.index:
            for prof in self.df_prof['id']:
                if course_alloc.loc[course, 'professor_id'] == prof:
                    prof_sel[prof, course] = 1
                else:
                    prof_sel[prof, course] = 0
        logging.info("Scheduler: Prof course mapping matrix computed successfully")
        return prof_sel



    def course_req(self):
        #print(pd.Series(self.df_courses["contact_hours"].values, index=self.df_courses["id"]).to_dict())
        return pd.Series(self.df_courses["contact_hours"].values, index=self.df_courses["id"]).to_dict()

    def class_capa(self):
        #print(pd.Series(self.df_class["capacity"].values, index=self.df_class["id"]).to_dict())
        return pd.Series(self.df_class["capacity"].values, index=self.df_class["id"]).to_dict()

    def stu_enrl(self):
        #print(pd.Series(self.tables_dataframes['professor_course_allocations']['maximum_students'].values, index=self.df_courses["id"]).to_dict())
        return pd.Series(self.tables_dataframes['professor_course_allocations']['maximum_students'].values, index=self.df_courses["id"]).to_dict()

    def create_model(self):
        model = pe.ConcreteModel()
        logging.info("Scheduler: Concrete Pyomo model instance created")
        # Model Data


        #Sets declaration:
        # List of classrooms that are available
        model.CLASSROOMS = pe.Set(initialize=self.df_class["id"].tolist())
        # Number of sessions per day
        model.SESSIONS = pe.Set(initialize=[1,2,3,4,5,6,7])
        # The days of the week available for scheduling sessions
        model.DAYS = pe.Set(initialize=['Monday','Tuesday','Wednesday','Thursday','Friday'])
        # The set of professors
        model.PROFS = pe.Set(initialize=self.df_prof["id"].tolist())
        # The set of courses available in that sem
        model.COURSES = pe.Set(initialize=self.df_courses["id"].tolist())


        #Parameter Declarations:

        #The hour requriement for a course for a week
        model.HREQ = pe.Param(model.COURSES, initialize=self.course_req())
        #The distance btw the classrooms:
        model.DIST = pe.Param(model.CLASSROOMS,model.CLASSROOMS, initialize=self.distance_btw_classes())
        #The distance btw the classrooms and profs office:
        model.DISTOFF = pe.Param(model.PROFS,model.CLASSROOMS, initialize=self.distance_btw_classoff())
        # Binary parameter to identify courses taken by a Prof:
        #model.PRFI=pe.Param(model.PROFS,model.COURSES, initialize={('SMD',60054):1,('CMD',60056):1,('SMD',60069):1,('SMD',64343):1},default= 0)
        model.PRFI=pe.Param(model.PROFS,model.COURSES, initialize=self.prof_courses())


        #The classroom capacity:
        #print(pd.Series(self.df_class["capacity"].values, index=self.df_class["id"]).to_dict())
        model.CLASSCAPA = pe.Param(model.CLASSROOMS, initialize=self.class_capa())
        #Students enrolled for a course:
        model.STUEN = pe.Param(model.COURSES, initialize=self.stu_enrl())


        #Variable declarations:
        #The selection variable:
        model.SELECTION = pe.Var(model.CLASSROOMS,model.SESSIONS,model.DAYS,model.COURSES,domain=pe.Binary )


        def fix_selection_variables(model, df):
            for index, row in df.iterrows():
                classroom = row['classroom_id']
                session = row['session']
                day = row['day']
                course = row['course_id']
                fix_value = row['condition']

                if fix_value:  # Fix variable if 'FIX' is True
                    model.SELECTION[classroom, session, day, course].fix(1)  # Fix to 1 for selection

        fix_selection_variables(model, self.df_availability)

        #Ensuring only class rooms with sufficient capacity if alloted to courses by fixing variable values:
        # for a in model.CLASSROOMS:
        #     for d in model.COURSES:
        #         if model.CLASSCAPA[a]-model.STUEN[d]<0:
        #             for b in model.SESSIONS:
        #                 for c in model.DAYS:
        #                     model.SELECTION[a,b,c,d].fixed=True
        #                     model.SELECTION[a,b,c,d].value = 0

        #Ensuring no classes are taken when Prof is not available
        # for i in self.df_prof.index:
        #     for a in model.COURSES:
        #         if model.PRFI[self.df_prof.loc[i,'id'],a]!=0:
        #             for c in model.DAYS:
        #                 for b in model.SESSIONS:
        #                     if self.df_prof.loc[i,'Day_unavail']== c:
        #                         if self.df_prof.loc[i,'Session_unavail']=='Mrng':
        #                             if 0<b<4:
        #                                 for d in model.CLASSROOMS:
        #                                     model.SELECTION[d, b, c, a].fixed = True
        #                                     model.SELECTION[d, b, c, a].value = 0
        #                         elif self.df_prof.loc[i,'Session_unavail']=='Aft':
        #                             if b>3:
        #                                 model.SELECTION[d, b, c, a].fixed = True
        #                                 model.SELECTION[d, b, c, a].value = 0





        # Objective
        def objective_function(model):
            for a in model.CLASSROOMS:
                for b in model.SESSIONS:
                    for c in model.DAYS:
                        for d in model.COURSES:

                            # if b != model.SESSIONS.first():

                            #     obj = sum(model.PRFI[e,d] * model.SELECTION[a, b, c, d] * sum(
                            #         model.SELECTION[a1, b - 1, c, d] * model.DIST[a, a1] for a1 in model.CLASSROOMS )for e in model.PROFS)
                            # else:
                            obj = sum(model.PRFI[e,d] * model.SELECTION[a, b, c, d] * model.DISTOFF[e,a] for e in model.PROFS)
            return obj
        model.OBJECTIVE = pe.Objective(rule=objective_function, sense=pe.minimize )
        # print(model.OBJECTIVE.pprint())

        # Constraints

        # Weekly requirement for courses need to be met:
        def course_week_req(model, course):
            return sum(model.SELECTION[a,b,c,course] for a in model.CLASSROOMS for b in model.SESSIONS for c in model.DAYS)==model.HREQ[course]
        model.WEEK_REQ = pe.Constraint(model.COURSES, rule=course_week_req)

        # A class can only has one session at a time:
        def class_clash(model, room,sessio,day):
            return sum(model.SELECTION[room,sessio,day,course] for course in model.COURSES)<=1
        model.CLASS_CLASH = pe.Constraint(model.CLASSROOMS, model.SESSIONS, model.DAYS, rule=class_clash)

        # A particular course can happen only at a single class at any particular time
        def course_clash(model, course,sessio,day):
            return sum(model.SELECTION[room, sessio, day, course] for room in model.CLASSROOMS) <= 1
        model.COURSE_CLASH = pe.Constraint(model.COURSES, model.SESSIONS, model.DAYS, rule=course_clash)

        #No more than 2 sessions per day for a course
        def sess_consec(model,room,day,course):
            return sum(model.SELECTION[room,sessio,day,course] for sessio in model.SESSIONS)<=2
        model.SESSION_CONS=pe.Constraint(model.CLASSROOMS, model.DAYS,model.COURSES, rule=sess_consec)

        #BigM tranformation
        pe.TransformationFactory("gdp.bigm").apply_to(model)
        logging.info("Scheduler: Model trnaformed to linear using BigM")
        #model.SESSION_CONS.pprint()
        return model

    def solve(self, solver_name, options=None, solver_path=None, local=True):

        if solver_path is not None:
            solver = pe.SolverFactory(solver_name, executable=solver_path,tee=True)
        else:
            solver = pe.SolverFactory(solver_name+"nl",executable=modules.find(solver_name),solver_io="nl")        
        logging.info("Scheduler: Solver:"+solver_name+" found for solving")
        
        if options is not None:
            for key, value in options.items():
                solver.options[key] = value

        if local:
            solver_results = solver.solve(self.model, tee=True,options=options)
            logging.info("Scheduler: Problem solved using local solver instance")
        else:
            solver_manager = pe.SolverManagerFactory("neos")
            solver_results = solver_manager.solve(self.model, opt=solver_name)
            logging.info("Scheduler: Problem sent to neos server to be solved using :"+solver_name)

        results = [[[a,b,c,d],self.model.SELECTION[a,b,c,d].value] for a in self.model.CLASSROOMS for b in self.model.SESSIONS for c in self.model.DAYS for d in self.model.COURSES ]
        #self.df_times = pd.DataFrame(results)


        prf = pd.DataFrame(columns=["professor_id", "course_id", "day", "start_time", "classroom_id"])
        for a in self.model.PROFS:
            for d in self.model.COURSES:
                if self.model.PRFI[a, d] == 1:
                    for c in self.model.DAYS:
                        for b in self.model.SESSIONS:
                            for e in self.model.CLASSROOMS:
                                # print(self.model.SELECTION[e, b, c, d].value == 1)
                                if self.model.SELECTION[e, b, c, d].value == 1:
                                    new_row = pd.DataFrame(
                                        {"professor_id": [a], "course_id": [d], "day": [c], "start_time": [b],
                                         "classroom_id": [e]})
                                    prf = pd.concat([
                                        prf, new_row], ignore_index=True)


        print(prf)



        #Write the result to table for each class
        self.postgres_connector.write_dataframe_to_postgres(prf,'professor_course_schedules')
        logging.info("Scheduler: Results written to db successfully")



if __name__ == "__main__":
    #Provide solver options
    options = {"maxit":10,"tol":1}
    scheduler = classScheduler()
    scheduler.solve(solver_name="bonmin", local=True, options=options)

