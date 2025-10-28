import Dashboard from './components/Dashboard';

function App() {
  return (
    <div className="min-h-screen w-full bg-gray-900 flex">
      {/* Left Panel: Image (optional, you can remove this section too) */}
      {/* <div 
        className="hidden lg:block lg:w-1/2 bg-cover bg-center"
        style={{ 
          backgroundImage: "url(https://cdn.prod.website-files.com/6567ab1461039867a87486d8/66a9973471f9bf6d4e0caf4f_65b9cf9f84649ea6eb40f342_ai-stock-prediction.png)" 
        }}
      /> */}

      {/* Right Panel: Dashboard (Main App) */}
      <div className="w-full  flex items-center justify-center ">
        <Dashboard />
      </div>
    </div>
  );
}

export default App;
