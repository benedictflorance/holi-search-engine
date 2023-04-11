import React, { createContext, useState, useRef } from 'react';
import { useNavigate } from 'react-router-dom';
import App from '../../App';

const AppContext = createContext();

const AppContextProvider = ({children}) => {
    const navigate = useNavigate();
    const [searchTerm, setSearchTerm] = useState('');
    const [searchResults, setSearchResults] = useState([]);
    const [isLoadingResults, setIsLoadingResults] = useState(false);

    const generateResults = async (searchTerm) => {
        setIsLoadingResults(true);
        let results = await fetch('http://localhost:8080/search').then(function(response) {
            // return response.text();
            return response.json();
          }).then(function(data) {
            return data;
          });
        console.log(results);
        setIsLoadingResults(false);
        return results;
        // let testdata = [
        //                 {title: "title 1", text: "http://simple.crawltest.cis5550.net:80/Cv1epgGc.html"}, 
        //                 {title: "title 2", text: "http://simple.crawltest.cis5550.net:80/ItU5tEu.html"},
        //                 {title: "title 3", text: "http://simple.crawltest.cis5550.net:80/ItU5tEu.html"},
        //                 {title: "title 4", text: "http://simple.crawltest.cis5550.net:80/ItU5tEu.html"}
        //             ];
        // return testdata;
    };

    const getResults = async (term) => {
        setSearchResults([]);
        let results = await generateResults(term);
        let numberOfResults = results.length;
        setSearchResults(results);
        if (numberOfResults < 3) getMoreResults(numberOfResults);
    };

    const getMoreResults = async (numberOfPrevResults) => {
        let newResults = await generateResults(searchTerm);
        let numberOfResults = numberOfPrevResults + newResults.length;

        setSearchResults(prev => [...prev, ...newResults]);

        if (numberOfResults < 3) getMoreResults(numberOfResults);
    }

    const holiSearch = (term) => {
        if (term.trim().length > 0) {
            navigate('/results?search=' + term.trim());

            getResults(term.trim());
        }
    }

    return (
        <AppContext.Provider value={{
            searchResults,
            setSearchResults,
            searchTerm,
            setSearchTerm,
            isLoadingResults,
            holiSearch
        }}>
            { children }
        </AppContext.Provider>
    );
};

export { AppContext, AppContextProvider };